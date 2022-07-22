{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

module Transient.SqlQuery
    where

import           Control.Monad                 (join)
import           Control.Monad.State           (State, get, put, runState)
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder       as BSB
import qualified Data.ByteString.Lazy          as BSL
import           Data.Foldable                 (fold, foldl')
import           Data.Function                 ((&))
import qualified Data.List                     as List
import           Data.List.NonEmpty            (NonEmpty)
import qualified Data.List.NonEmpty            as NE
import           Data.Map                      (Map)
import qualified Data.Map                      as Map
import           Data.Maybe                    (listToMaybe, mapMaybe)
import           Data.String                   (IsString (..))

import           Transient.Core
import           Transient.Internal.Backend
import           Transient.Internal.SqlDialect


-- Imports for Test
import           Data.Int                      (Int64)
import           Data.Void                     (Void, absurd)

type QueryFragment = SqlDialect -> (BSB.Builder, [SqlValue])

instance IsString (BSB.Builder, [SqlValue]) where
    fromString s = (fromString s, [])

data Nullable
data NotNullable
type SqlExpr a = SqlExpr' NotNullable a
data SqlExpr' nullable a = SqlExpr
    { sqlExprKind    :: SqlExprKind
    , sqlExprDecoder :: Decoder a
    } deriving Functor

sqlExprSelect :: SqlExpr' nullable a -> SqlSelect a
sqlExprSelect expr =
  SqlSelect
    { sqlSelectDecoder = sqlExprDecoder expr
    , sqlSelectQueryFragments = NE.toList $ sqlExprMultiFragment expr
    }

data SqlExprKind = SqlExprKind
    { sqlExprKindFragments     :: NonEmpty QueryFragment
    , sqlExprKindAliased       :: Ident -> Ident -> SqlExprKind
    , sqlExprKindAliasedSelect :: Ident -> [QueryFragment]
    , sqlExprKindProjectField  :: Ident -> QueryFragment
    }

renderQualifiedName :: Ident -> Ident -> QueryFragment
renderQualifiedName baseIdent ident d =
    (renderIdent d baseIdent <> "." <> renderIdent d ident, [])

valueExpression :: QueryFragment -> SqlExprKind
valueExpression frag =
    SqlExprKind
        { sqlExprKindFragments = NE.singleton frag
        , sqlExprKindAliased = \subqueryIdent aliasIdent ->
            valueExpression (renderQualifiedName subqueryIdent aliasIdent)
        , sqlExprKindAliasedSelect = \aliasIdent ->
            [\d -> frag d <> " AS " <> (renderIdent d aliasIdent, [])]
        , sqlExprKindProjectField = \_ -> frag
        }

tableExpression :: Ident -> NonEmpty Ident -> SqlExprKind
tableExpression tableIdent fieldIdents =
    SqlExprKind
        { sqlExprKindFragments = fmap (renderQualifiedName tableIdent) fieldIdents
        , sqlExprKindAliased = \subqueryIdent aliasIdent ->
            aliasedTableReference subqueryIdent aliasIdent fieldIdents
        , sqlExprKindAliasedSelect = \aliasIdent ->
            NE.toList $ fmap (\field d ->
                (renderQualifiedName tableIdent field d
                 <> " AS " <> (renderIdent d (aliasIdent <> "_" <> field), []))) fieldIdents
        , sqlExprKindProjectField = renderQualifiedName tableIdent
        }

aliasedTableReference :: Ident -> Ident -> NonEmpty Ident -> SqlExprKind
aliasedTableReference subqueryIdent aliasIdent fieldIdents =
    let aliasField field = aliasIdent <> "_" <> field
    in SqlExprKind
        { sqlExprKindFragments = fmap (renderQualifiedName subqueryIdent . aliasField) fieldIdents
        , sqlExprKindAliased = \subqueryIdent' aliasIdent' ->
            aliasedTableReference subqueryIdent' aliasIdent' fieldIdents
        , sqlExprKindAliasedSelect = \newAliasIdent ->
            NE.toList $ fmap (\field d ->
                (renderQualifiedName subqueryIdent (aliasField field) d
                 <> " AS " <> (renderIdent d (newAliasIdent <> "_" <> field), []))) fieldIdents
        , sqlExprKindProjectField = \fieldIdent ->
            renderQualifiedName subqueryIdent (aliasIdent <> "_" <> fieldIdent)
        }

sqlExprFragment :: SqlExpr' n a -> QueryFragment
sqlExprFragment expr =
    NE.head $ sqlExprMultiFragment expr

sqlExprMultiFragment :: SqlExpr' n a -> NonEmpty QueryFragment
sqlExprMultiFragment expr =
    sqlExprKindFragments $ sqlExprKind expr

newtype From a = From
    { unFrom :: SqlQuery (QueryFragment, a) }

class AsFrom f r | f -> r where
    asFrom :: f -> From r
instance AsFrom (From a) a where
    asFrom = id

data SqlQueryState = SqlQueryState
    { sqsQueryParts    :: SqlQueryData
    , sqsIdentifierMap :: SqlQueryIdentifierMap
    }

newtype Ident = Ident { unIdent :: BS.ByteString }
    deriving newtype (Eq, IsString, Semigroup)

unsafeIdentFromString :: BS.ByteString -> Ident
unsafeIdentFromString = Ident

renderIdent :: SqlDialect -> Ident -> BSB.Builder
renderIdent dialect ident =
    BSB.byteString $ dialectEscapeIdentifier dialect $ unIdent ident

type SqlQueryIdentifierMap = Map BS.ByteString Int

freshIdent :: BS.ByteString -> SqlQuery Ident
freshIdent baseIdent = SqlQuery $ do
    sqs <- get
    case Map.lookup baseIdent (sqsIdentifierMap sqs) of
      Just count -> do
          let nextCount = count + 1
              ident = baseIdent <> (fromString (show nextCount))
              nextMap = Map.insert baseIdent nextCount $ sqsIdentifierMap sqs
          put sqs{sqsIdentifierMap = nextMap}
          pure $ unsafeIdentFromString ident
      Nothing -> do
          let nextMap = Map.insert baseIdent 1 $ sqsIdentifierMap sqs
          put sqs{sqsIdentifierMap = nextMap}
          pure $ unsafeIdentFromString baseIdent

data SqlQueryData = SqlQueryData
    { sqlQueryDataFromClause   :: Maybe [QueryFragment]
    , sqlQueryDataWhereClause  :: Maybe [QueryFragment]
    , sqlQueryDataHavingClause :: Maybe [QueryFragment]
    }

newtype SqlQuery a = SqlQuery
    { unSqlQuery :: State SqlQueryState a }
    deriving newtype (Functor, Applicative, Monad)

instance Semigroup SqlQueryData where
    (SqlQueryData a b c) <> (SqlQueryData a' b' c') = SqlQueryData (a <> a') (b <> b') (c <> c')
instance Monoid SqlQueryData where
    mempty = SqlQueryData mempty mempty mempty
    mappend = (<>)

data SqlSelect a = SqlSelect
    { sqlSelectQueryFragments :: [QueryFragment]
    , sqlSelectDecoder        :: Decoder a
    } deriving Functor

instance Applicative SqlSelect where
    pure a =
        SqlSelect
            { sqlSelectQueryFragments = mempty
            , sqlSelectDecoder = const $ Just (a, [])
            }
    a <*> b =
        SqlSelect
            { sqlSelectQueryFragments = sqlSelectQueryFragments a <> sqlSelectQueryFragments b
            , sqlSelectDecoder = \vs -> do
                (f, v2) <- sqlSelectDecoder a vs
                (x, vRest) <- sqlSelectDecoder b v2
                pure (f x, vRest)
            }

data AliasedReference a = AliasedReference
    { aliasedSelectQueryFragment :: [QueryFragment]
    , aliasedValue               :: Ident -> a
    } deriving Functor

instance Applicative AliasedReference where
    pure a =
        AliasedReference
            { aliasedSelectQueryFragment = mempty
            , aliasedValue = const a
            }
    f <*> a =
        AliasedReference
            { aliasedSelectQueryFragment = aliasedSelectQueryFragment f <> aliasedSelectQueryFragment a
            , aliasedValue = aliasedValue f <*> aliasedValue a
            }

class AsAliasedReference a where
    alias_ :: a -> SqlQuery (AliasedReference a)

instance AsAliasedReference (SqlExpr' n a) where
    alias_ expr = do
        aliasIdent <- freshIdent "v"
        pure $ AliasedReference
            { aliasedValue = \subqueryIdent ->
                SqlExpr
                    { sqlExprDecoder = sqlExprDecoder expr
                    , sqlExprKind = sqlExprKindAliased (sqlExprKind expr) subqueryIdent aliasIdent
                    }
            , aliasedSelectQueryFragment = sqlExprKindAliasedSelect (sqlExprKind expr) aliasIdent
            }

instance (AsAliasedReference a, AsAliasedReference b) => AsAliasedReference (a :& b) where
    alias_ (a :& b) = do
        a' <- alias_ a
        b' <- alias_ b
        pure $ (:&) <$> a' <*> b'

maybeDecoder :: Int -> Decoder a -> Decoder (Maybe a)
maybeDecoder columnCount decoder vals =
    let (columns, rest) = splitAt columnCount vals
    in
    if (all (== SqlNull) columns) then
        pure (Nothing, rest)
    else do
        (x, []) <- decoder columns
        pure (Just x, rest)

nullableSqlSelect :: SqlSelect a -> SqlSelect (Maybe a)
nullableSqlSelect base =
    base{ sqlSelectDecoder = maybeDecoder (length $ sqlSelectQueryFragments base) (sqlSelectDecoder base) }

select :: AsSelect a r => a -> SelectQuery r
select = pure . select_

class AsSelect a r | a -> r where
    select_ :: a -> SqlSelect r
instance AsSelect (SqlSelect a) a where
    select_ = id
instance AsSelect (SqlExpr' NotNullable a) a where
    select_ = sqlExprSelect
instance AsSelect (SqlExpr' Nullable a) (Maybe a) where
    select_ = nullableSqlSelect . sqlExprSelect
instance (AsSelect a ar, AsSelect b br) => AsSelect (a :& b) (ar :& br) where
    select_ (a :& b) =
        (:&) <$> select_ a <*> select_ b

type SelectQuery r = SqlQuery (SqlSelect r)

runSelectUnique :: Backend -> SelectQuery r -> IO (Maybe r)
runSelectUnique conn q = fmap listToMaybe $ runSelectMany conn q

unSelectQuery :: SelectQuery a -> (SqlSelect a, SqlQueryState)
unSelectQuery q =
    runState (unSqlQuery q) (SqlQueryState mempty mempty)

runSelectMany :: Backend -> SelectQuery r -> IO [r]
runSelectMany conn q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        (qText, qVals) = renderSelect (connDialect conn) (sqlSelectQueryFragments sqlSelect) qData
    in do
        rVals <- connQuery conn qText qVals
        pure $ flip mapMaybe rVals $ \vals -> do
            (v, []) <- sqlSelectDecoder sqlSelect vals
            pure v

uncommas :: [QueryFragment] -> QueryFragment
uncommas frags dialect =
    fold $ List.intersperse ", " $
    fmap ($ dialect) frags

renderSelect :: SqlDialect -> [QueryFragment] -> SqlQueryData -> (BS.ByteString, [SqlValue])
renderSelect d selectFragments qd =
    let (text, vals) =
            fold
                [ "SELECT "
                , uncommas selectFragments d
                , maybe mempty (" FROM " <>) (fmap (fold . List.intersperse " CROSS JOIN LATERAL " . fmap ($ d)) $ sqlQueryDataFromClause qd)
                , maybe mempty (" WHERE " <>) (fmap (fold . List.intersperse " AND " . fmap ($ d)) $ sqlQueryDataWhereClause qd)
                , maybe mempty (" HAVING " <>) (fmap (fold . List.intersperse " AND " . fmap ($ d)) $ sqlQueryDataHavingClause qd)
                ]
    in (BSL.toStrict $ BSB.toLazyByteString text, vals)

from :: AsFrom f r => f -> SqlQuery r
from f = do
    (fromClause, res) <- unFrom $ asFrom f
    writeQueryData mempty{sqlQueryDataFromClause = Just [fromClause]}
    pure res

writeQueryData :: SqlQueryData -> SqlQuery ()
writeQueryData queryData =
    SqlQuery $ do
        sqs <- get
        put $ sqs{ sqsQueryParts = sqsQueryParts sqs <> queryData }

where_ :: SqlExpr' NotNullable Bool -> SqlQuery ()
where_ expr =
    writeQueryData mempty{sqlQueryDataWhereClause = Just [sqlExprFragment expr]}

simpleExpr :: QueryFragment -> Decoder a -> SqlExpr' n a
simpleExpr frag fromRow =
    SqlExpr
        { sqlExprDecoder = fromRow
        , sqlExprKind = valueExpression frag
        }

binOp :: BSB.Builder -> BSB.Builder -> Decoder c -> SqlExpr' n a -> SqlExpr' n b -> SqlExpr' n c
binOp op connect fromRow lhs rhs =
    let applyOp :: QueryFragment -> QueryFragment -> QueryFragment
        applyOp l r = \d -> l d <> (" " <> op <> " ", []) <> r d
        frag = fold $
               NE.intersperse (const (" " <> connect <> " ", [])) $
               fmap (uncurry applyOp) $
               NE.zip (sqlExprMultiFragment lhs) (sqlExprMultiFragment rhs)
    in simpleExpr frag fromRow

boolFromRow :: Decoder Bool
boolFromRow = fieldParser auto

infixr 6 ==., /=.
(==.) :: SqlExpr' n a -> SqlExpr' n a -> SqlExpr' n Bool
(==.) = binOp "=" "AND" boolFromRow
(/=.) :: SqlExpr' n a -> SqlExpr' n a -> SqlExpr' n Bool
(/=.) = binOp "!=" "OR" boolFromRow

infix 4 `and_`, `or_`
and_ :: SqlExpr' n Bool -> SqlExpr' n Bool -> SqlExpr' n Bool
and_ = binOp "AND" "AND" boolFromRow
or_ :: SqlExpr' n Bool -> SqlExpr' n Bool -> SqlExpr' n Bool
or_ = binOp "OR" "OR" boolFromRow

isNull_ :: SqlExpr' Nullable a -> SqlExpr' n Bool
isNull_ expr =
    let frag = fold $
               NE.intersperse (const " AND ") $
               fmap (<> const " IS NULL") $
               sqlExprMultiFragment expr
    in simpleExpr frag boolFromRow

isNotNull_ :: SqlExpr' Nullable a -> SqlExpr' NotNullable Bool
isNotNull_ expr =
    let frag = fold $
               NE.intersperse (const " OR ") $
               fmap (<> const " IS NOT NULL") $
               sqlExprMultiFragment expr
    in simpleExpr frag boolFromRow

val :: AutoType i => i -> SqlExpr' NotNullable i
val x = valOfType x auto

valOfType :: i -> SqlType' i o -> SqlExpr' NotNullable o
valOfType x sqlType =
    let v = case toSqlType sqlType x of
                SqlDefault -> SqlNull -- Default should only be used by insert many
                x          -> x
    in simpleExpr (const ("?", [v])) (fieldParser sqlType)

just_ :: SqlExpr' n a -> SqlExpr' Nullable a
just_ expr =
    SqlExpr
        { sqlExprKind = sqlExprKind expr
        , sqlExprDecoder = sqlExprDecoder expr
        }

nullable_ :: SqlExpr' n (Maybe a) -> SqlExpr' Nullable a
nullable_ expr =
    SqlExpr
        { sqlExprKind = sqlExprKind expr
        , sqlExprDecoder = \values -> do
            (Just res, rest) <- sqlExprDecoder expr values
            pure (res, rest)
        }

instance AsFrom (Table i r) (SqlExpr' NotNullable r) where
    asFrom = table_

table_ :: Table x r -> From (SqlExpr' NotNullable r)
table_ model = From $ do
    ident <- freshIdent (tableName model)
    let originalTableIdent = unsafeIdentFromString (tableName model)
        fromFragment = if ident == originalTableIdent
                          then \d -> (renderIdent d ident, [])
                          else \d -> (renderIdent d originalTableIdent <> " AS " <> renderIdent d ident, [])
        fieldIdent field =
            unsafeIdentFromString (fieldName field)
        qualifiedFieldFrag field d =
            (renderIdent d ident <> "." <> renderIdent d (fieldIdent field), [])
        unqualifiedFields = fmap fieldIdent $ tableFields model
        qualifiedFields = fmap qualifiedFieldFrag $ tableFields model
    pure ( fromFragment
         , SqlExpr
            { sqlExprKind = tableExpression ident (NE.fromList unqualifiedFields)
            , sqlExprDecoder = tableDecoder model
            }
         )

instance AsFrom (SqlQuery (AliasedReference a)) a where
    asFrom = subquery_

subquery_ :: SqlQuery (AliasedReference a) -> From a
subquery_ query = From $ do
    queryState <- SqlQuery get
    SqlQuery $ put $ queryState{ sqsQueryParts = mempty }
    ref <- query
    subqueryState <- SqlQuery get
    SqlQuery $ put queryState{ sqsIdentifierMap = sqsIdentifierMap subqueryState }
    subqueryIdent <- freshIdent "q"
    let subqueryFragment dialect =
            let (text, values) = renderSelect dialect (aliasedSelectQueryFragment ref) (sqsQueryParts subqueryState)
            in (BSB.byteString text, values)
    pure ( \d -> "(" <> subqueryFragment d <> ")" <> " AS " <> (renderIdent d subqueryIdent, [])
         , aliasedValue ref subqueryIdent
         )

type OnClause a = a -> SqlExpr' NotNullable Bool

infix 9 `on_`
on_ :: (OnClause a -> b) -> OnClause a -> b
on_ = ($)

data a :& b = a :& b
    deriving (Show, Eq)
infixl 2 :&

class ToMaybe arg result | arg -> result where
    toMaybe :: arg -> result

instance ToMaybe (SqlExpr' n a) (SqlExpr' Nullable a) where
    toMaybe = just_

instance (ToMaybe a a', ToMaybe b b') => ToMaybe (a :& b) (a' :& b') where
    toMaybe (a :& b) = toMaybe a :& toMaybe b

type Join fa a fb b c = fb -> OnClause (a :& b) -> fa -> From c

joinHelper :: (AsFrom fa a, AsFrom fb b)
           => BSB.Builder
           -> (a -> b -> c)
           -> Join fa a fb b c
joinHelper joinType toRes joinFrom onClause baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    let onExpr = onClause (baseExpr :& joinExpr)
    pure ( \d -> baseFromFragment d <> (" " <> joinType <> " ", []) <> joinFromFragment d <> " ON " <> sqlExprFragment onExpr d
         , toRes baseExpr joinExpr
         )

innerJoin_ :: (AsFrom fa a, AsFrom fb b) => Join fa a fb b (a :& b)
innerJoin_ = joinHelper "INNER JOIN" (:&)

leftJoin_ :: (AsFrom fa a, AsFrom fb b, ToMaybe b mb) => Join fa a fb b (a :& mb)
leftJoin_ = joinHelper "LEFT JOIN" (\a b -> a :& toMaybe b)

infixr 9 ^., ?., ??.
(^.) :: SqlExpr rec -> EntityField rec i o -> SqlExpr o
(^.) = projectField

(?.) :: SqlExpr' Nullable rec -> EntityField rec i o -> SqlExpr' Nullable o
(?.) = projectField

(??.) :: SqlExpr' Nullable rec -> EntityField rec i (Maybe o) -> SqlExpr' Nullable o
(??.) ent field = nullable_ $ ent ?. field

projectField :: SqlExpr' n a -> EntityField b c d -> SqlExpr' n' d
projectField ent (EntityField name sqlType) =
    let nameIdent = unsafeIdentFromString name
        frag = sqlExprKindProjectField (sqlExprKind ent) nameIdent
    in simpleExpr frag (fieldParser sqlType)

insertInto :: Backend -> Table w r -> SelectQuery r -> IO Int64
insertInto conn t q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        dialect = connDialect conn
        (qText, qVals) = renderSelect dialect (sqlSelectQueryFragments sqlSelect) qData
        insertText = "INSERT INTO " <> dialectEscapeIdentifier dialect (tableName t) <> " " <> qText
    in connExecute conn insertText qVals
