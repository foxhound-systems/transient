{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
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
    { sqlExprSelect    :: SqlSelect a
    , sqlExprKind      :: SqlExprKind 
    , sqlExprAliasBase :: Maybe Ident
    } deriving Functor

sqlExprFragment :: SqlExpr' n a -> QueryFragment
sqlExprFragment expr =
    case sqlExprKind expr of
      ValueExpression frag -> frag
      TableExpression ident _ -> \d -> (renderIdent d ident, [])

sqlExprMultiFragment :: SqlExpr' n a -> Maybe (NonEmpty QueryFragment)
sqlExprMultiFragment expr =
    case sqlExprKind expr of
      ValueExpression _ -> Nothing 
      TableExpression _ fields -> Just $ fmap (\field d -> (renderIdent d field, [])) fields

data SqlExprKind 
    = ValueExpression QueryFragment
    | TableExpression Ident (NonEmpty Ident)

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
    , sqlSelectFromRow        :: FromRowFn a
    , sqlSelectColumnCount    :: Int
    } deriving Functor

instance Applicative SqlSelect where
    pure a =
        SqlSelect
            { sqlSelectQueryFragments = mempty
            , sqlSelectFromRow = const $ Just (a, [])
            , sqlSelectColumnCount = 0
            }
    a <*> b =
        SqlSelect
            { sqlSelectQueryFragments = sqlSelectQueryFragments a <> sqlSelectQueryFragments b
            , sqlSelectFromRow = \vs -> do
                (f, v2) <- sqlSelectFromRow a vs
                (x, vRest) <- sqlSelectFromRow b v2
                pure (f x, vRest)
            , sqlSelectColumnCount = sqlSelectColumnCount a + sqlSelectColumnCount b
            }

sqlSelectExpectValue :: SqlSelect (Maybe a) -> SqlSelect a
sqlSelectExpectValue sqlSelect =
    SqlSelect
        { sqlSelectQueryFragments = sqlSelectQueryFragments sqlSelect
        , sqlSelectColumnCount = sqlSelectColumnCount sqlSelect
        , sqlSelectFromRow = \values -> do
            (Just res, rest) <- sqlSelectFromRow sqlSelect values
            pure (res, rest)
        }

data AliasedReference a = AliasedReference
    { aliasedSelectQueryFragment :: [QueryFragment]
    , aliasedValue :: Ident -> a
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
        case sqlExprKind expr of
            ValueExpression frag -> 
                  let identFrag d = (renderIdent d aliasIdent, [])
                      aliasExpr subqueryIdent = 
                        let fullyQualifiedIdentFrag d = 
                                (renderIdent d subqueryIdent <> "." <> renderIdent d aliasIdent, [])
                        in SqlExpr
                            { sqlExprSelect = 
                                (sqlExprSelect expr)
                                    { sqlSelectQueryFragments = [fullyQualifiedIdentFrag] 
                                    }
                            , sqlExprKind = ValueExpression fullyQualifiedIdentFrag 
                            , sqlExprAliasBase = Nothing
                            }
                 in pure $ AliasedReference
                    { aliasedValue = aliasExpr
                    , aliasedSelectQueryFragment = [\d -> frag d <> " AS " <> identFrag d]
                    }
            TableExpression tableIdent fields ->
                 let aliasField field = aliasIdent <> "_" <> field
                     aliasedFields = fmap aliasField fields
                 in pure $ AliasedReference
                    { aliasedValue = \subqueryIdent ->
                        let fullyQualifiedAliasedFields = 
                                fmap (\field d -> (renderIdent d subqueryIdent <> "." <> renderIdent d field, [])) aliasedFields
                        in SqlExpr 
                            { sqlExprKind = TableExpression subqueryIdent aliasedFields
                            , sqlExprAliasBase = Just aliasIdent 
                            , sqlExprSelect = 
                                 (sqlExprSelect expr)
                                    { sqlSelectQueryFragments = NE.toList fullyQualifiedAliasedFields
                                    }
                            }
                    , aliasedSelectQueryFragment = 
                        NE.toList $ fmap (\field d -> 
                            (renderIdent d tableIdent <> "." <> renderIdent d field 
                             <> " AS " <> renderIdent d (aliasField field), [])) fields
                    }

instance (AsAliasedReference a, AsAliasedReference b) => AsAliasedReference (a :& b) where 
    alias_ (a :& b) = do 
        a' <- alias_ a 
        b' <- alias_ b
        pure $ (:&) <$> a' <*> b'

maybeFromRow :: Int -> FromRowFn a -> FromRowFn (Maybe a)
maybeFromRow columnCount fromRow vals =
    if (all (== SqlNull) $ take columnCount vals) then
        pure (Nothing, drop columnCount vals)
    else do
        (x, r) <- fromRow vals
        pure (Just x, r)

nullableSqlSelect :: SqlSelect a -> SqlSelect (Maybe a)
nullableSqlSelect base =
    base{ sqlSelectFromRow = maybeFromRow (sqlSelectColumnCount base) (sqlSelectFromRow base) }

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

instance (AsSelect a ar, AsSelect b br) => AsSelect (a, b) (ar, br) where
    select_ (a, b) =
            (,) <$> select_ a <*> select_ b

instance (AsSelect a ar, AsSelect b br, AsSelect c cr) => AsSelect (a, b, c) (ar, br, cr) where
    select_ (a, b, c) = (,,) <$> select_ a <*> select_ b <*> select_ c

instance ( AsSelect a ar , AsSelect b br
         , AsSelect c cr , AsSelect d dr
         ) => AsSelect (a, b, c, d) (ar, br, cr, dr) where
    select_ (a, b, c, d) =
                (,,,) <$> select_ a <*> select_ b
                      <*> select_ c <*> select_ d

instance ( AsSelect a ar , AsSelect b br
         , AsSelect c cr , AsSelect d dr
         , AsSelect e er
         ) => AsSelect (a, b, c, d, e) (ar, br, cr, dr, er) where
    select_ (a, b, c, d, e) =
                (,,,,) <$> select_ a <*> select_ b
                       <*> select_ c <*> select_ d
                       <*> select_ e

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr
         , AsSelect d dr , AsSelect e er , AsSelect f fr
         ) => AsSelect (a, b, c, d, e, f) (ar, br, cr, dr, er, fr) where
    select_ (a, b, c, d, e, f) =
                (,,,,,) <$> select_ a <*> select_ b <*> select_ c
                        <*> select_ d <*> select_ e <*> select_ f

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr , AsSelect d dr
         , AsSelect e er , AsSelect f fr , AsSelect g gr
         ) => AsSelect (a, b, c, d, e, f, g) (ar, br, cr, dr, er, fr, gr) where
    select_ (a, b, c, d, e, f, g) =
                (,,,,,,) <$> select_ a <*> select_ b <*> select_ c <*> select_ d
                         <*> select_ e <*> select_ f <*> select_ g

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr , AsSelect d dr
         , AsSelect e er , AsSelect f fr , AsSelect g gr , AsSelect h hr
         ) => AsSelect (a, b, c, d, e, f, g, h) (ar, br, cr, dr, er, fr, gr, hr) where
    select_ (a, b, c, d, e, f, g, h) =
                (,,,,,,,) <$> select_ a <*> select_ b <*> select_ c <*> select_ d
                          <*> select_ e <*> select_ f <*> select_ g <*> select_ h

instance ( AsSelect a ar , AsSelect b br , AsSelect c cr
         , AsSelect d dr , AsSelect e er , AsSelect f fr
         , AsSelect g gr , AsSelect h hr , AsSelect i ir
         ) => AsSelect (a, b, c, d, e, f, g, h, i) (ar, br, cr, dr, er, fr, gr, hr, ir) where
    select_ (a, b, c, d, e, f, g, h, i) =
                (,,,,,,,,) <$> select_ a <*> select_ b <*> select_ c
                           <*> select_ d <*> select_ e <*> select_ f
                           <*> select_ g <*> select_ h <*> select_ i

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
        rVals <- qText `seq` connQuery conn qText qVals
        pure $ flip mapMaybe rVals $ \vals -> do
            (v, []) <- sqlSelectFromRow sqlSelect vals
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

simpleExpr :: QueryFragment -> FromRowFn a -> SqlExpr' n a
simpleExpr frag fromRow =
    SqlExpr
        { sqlExprSelect = SqlSelect [frag] fromRow 1
        , sqlExprKind = ValueExpression frag
        , sqlExprAliasBase = Nothing
        }

binOp :: BSB.Builder -> BSB.Builder-> FromRowFn c -> SqlExpr' n a -> SqlExpr' n b -> SqlExpr' n c
binOp op connect fromRow lhs rhs =
    let applyOp :: QueryFragment -> QueryFragment -> QueryFragment
        applyOp l r = \d -> l d <> (" " <> op <> " ", []) <> r d
        frag = case (sqlExprMultiFragment lhs, sqlExprMultiFragment rhs) of
              (Just ls, Just rs) ->
                       fold $
                       NE.intersperse (const (" " <> connect <> " ", [])) $
                       fmap (uncurry applyOp) $
                       NE.zip ls rs

              _ -> applyOp (sqlExprFragment lhs) (sqlExprFragment rhs)
    in simpleExpr frag fromRow

boolFromRow :: FromRowFn Bool
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
    let frag =
            case sqlExprMultiFragment expr of
              Just fs ->
                   fold $
                   NE.intersperse (const " AND ") $
                   fmap (<> const " IS NULL") fs
              Nothing ->
                   sqlExprFragment expr <> const " IS NULL"
    in simpleExpr frag boolFromRow

isNotNull_ :: SqlExpr' Nullable a -> SqlExpr' NotNullable Bool
isNotNull_ expr =
    let frag =
            case sqlExprMultiFragment expr of
              Just fs ->
                   fold $
                   NE.intersperse (const " OR ") $
                   fmap (<> const " IS NOT NULL") fs
              Nothing ->
                   sqlExprFragment expr <> const " IS NOT NULL"
    in simpleExpr frag boolFromRow

val :: AutoType i => i -> SqlExpr' NotNullable i
val x = val' x auto

val' :: i -> SqlType' i o -> SqlExpr' NotNullable o
val' x sqlType =
    let v = case toSqlType sqlType x of
                SqlDefault -> SqlNull -- Default should only be used by insert many
                x          -> x
    in simpleExpr (const ("?", [v])) (fieldParser sqlType)

just_ :: SqlExpr' n a -> SqlExpr' Nullable a
just_ expr = 
    SqlExpr 
        { sqlExprKind = sqlExprKind expr
        , sqlExprAliasBase = sqlExprAliasBase expr
        , sqlExprSelect = sqlExprSelect expr
        }

nullable_ :: SqlExpr' n (Maybe a) -> SqlExpr' Nullable a
nullable_ expr = 
    SqlExpr 
        { sqlExprKind = sqlExprKind expr
        , sqlExprAliasBase = sqlExprAliasBase expr
        , sqlExprSelect = sqlSelectExpectValue $ sqlExprSelect expr
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
            { sqlExprKind = TableExpression ident (NE.fromList unqualifiedFields)
            , sqlExprSelect = SqlSelect qualifiedFields (tableFromRow model) (length $ tableFields model)
            , sqlExprAliasBase = Nothing
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
    SqlQuery $ put queryState
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

leftJoin_ :: (AsFrom fa a, AsFrom fb b, ToMaybe b mb)
          => Join fa a fb b (a :& mb)
leftJoin_ = joinHelper "LEFT JOIN" (\a b -> a :& toMaybe b)

infixr 9 ^., ?., ??.
(^.) :: SqlExpr rec -> EntityField rec i o -> SqlExpr o
(^.) = projectField id

(?.) :: SqlExpr' Nullable rec -> EntityField rec i o -> SqlExpr' Nullable o
(?.) = projectField id 

(??.) :: SqlExpr' Nullable rec -> EntityField rec i (Maybe o) -> SqlExpr' Nullable o
(??.) ent field = nullable_ $ ent ?. field

projectField :: (FromRowFn d -> FromRowFn e) -> SqlExpr' n a -> EntityField b c d -> SqlExpr' n' e
projectField f ent (EntityField name sqlType) =
    let nameIdent = unsafeIdentFromString name
        fieldIdent = maybe nameIdent (\prefix -> prefix <> "_" <> nameIdent) (sqlExprAliasBase ent)
        frag = \d -> sqlExprFragment ent d <> "." <> (renderIdent d fieldIdent, [])
        fromRow = fieldParser sqlType
    in simpleExpr frag (f fromRow)

insertInto :: Backend -> Table w r -> SelectQuery r -> IO Int64
insertInto conn t q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        dialect = connDialect conn
        (qText, qVals) = renderSelect dialect (sqlSelectQueryFragments sqlSelect) qData
        insertText = "INSERT INTO " <> dialectEscapeIdentifier dialect (tableName t) <> " " <> qText
    in insertText `seq` connExecute conn insertText qVals
