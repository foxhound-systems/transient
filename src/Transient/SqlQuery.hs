{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedLabels           #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

module Transient.SqlQuery
    where

import           Control.Monad                 (join)
import           Control.Monad.State           (State, get, put, runState)
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder       as BSB
import qualified Data.ByteString.Char8         as C8
import qualified Data.ByteString.Lazy          as BSL
import           Data.Foldable                 (fold, foldl')
import           Data.Function                 ((&))
import qualified Data.List                     as List
import           Data.List.NonEmpty            (NonEmpty)
import qualified Data.List.NonEmpty            as NE
import           Data.Map                      (Map)
import qualified Data.Map                      as Map
import           Data.Maybe                    (fromMaybe, listToMaybe,
                                                mapMaybe)
import           Data.String                   (IsString (..))

import           Transient.Core
import           Transient.Internal.Backend
import           Transient.Internal.SqlDialect

import           Data.Proxy                    (Proxy (..))
import           GHC.OverloadedLabels          (IsLabel (..))
import           GHC.Records                   (HasField)
import           GHC.TypeLits                  (KnownSymbol, symbolVal)


-- Imports for Test
import           Data.Int                      (Int64)
import           Data.Void                     (Void, absurd)

type QueryFragment = SqlDialect -> (BSB.Builder, [SqlValue])

instance IsString (BSB.Builder, [SqlValue]) where
    fromString s = (fromString s, [])

data Nullable
data NotNullable
data SqlExpr nullable a = SqlExpr
    { sqlExprFragment  :: QueryFragment
    } deriving Functor

veryUnsafeCoerceSqlExpr :: SqlExpr n a -> SqlExpr n' a'
veryUnsafeCoerceSqlExpr (SqlExpr f) = SqlExpr f

data SqlRecord nullable a = SqlRecord
    { sqlRecordFieldMap  :: Map String Ident
    , sqlRecordDecoder   :: Decoder a
    , sqlRecordBaseIdent :: Ident
    , sqlRecordFields    :: NonEmpty Ident
    }

veryUnsafeCoerceSqlRecord :: SqlRecord n a -> SqlRecord n' a
veryUnsafeCoerceSqlRecord (SqlRecord a b c d) = SqlRecord a b c d

renderQualifiedName :: Ident -> Ident -> QueryFragment
renderQualifiedName baseIdent ident d =
    (renderIdent d baseIdent <> "." <> renderIdent d ident, [])

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

instance AsAliasedReference (SqlExpr n a) where
    alias_ = sqlExprAliasedReference

sqlExprAliasedReference :: SqlExpr n a -> SqlQuery (AliasedReference (SqlExpr n a))
sqlExprAliasedReference expr = do
    valueIdent <- freshIdent "v"
    pure $ AliasedReference
        { aliasedSelectQueryFragment =
            [ \d ->
                sqlExprFragment expr d <> " AS " <> (renderIdent d valueIdent, [])
            ]
        , aliasedValue = \subqueryIdent ->
            SqlExpr{
                sqlExprFragment = renderQualifiedName subqueryIdent valueIdent
            }
        }

instance AsAliasedReference (SqlRecord n a) where
    alias_ = sqlRecordAliasedReference

sqlRecordAliasedReference :: SqlRecord n a -> SqlQuery (AliasedReference (SqlRecord n a))
sqlRecordAliasedReference record = do
    let prefixedFieldIdent fieldIdent = sqlRecordBaseIdent record <> "_" <> fieldIdent
        aliasedFieldIdents = fmap prefixedFieldIdent (sqlRecordFields record)
        aliasedFieldMap = fmap prefixedFieldIdent (sqlRecordFieldMap record)
        renderField fieldIdent = \d ->
            renderQualifiedName (sqlRecordBaseIdent record) fieldIdent d
            <> " AS "
            <> (renderIdent d (prefixedFieldIdent fieldIdent), [])
    pure $ AliasedReference
        { aliasedSelectQueryFragment =
            NE.toList $ fmap renderField $ sqlRecordFields record
        , aliasedValue = \subqueryIdent ->
            record
                { sqlRecordFields = aliasedFieldIdents
                , sqlRecordFieldMap = aliasedFieldMap
                , sqlRecordBaseIdent = subqueryIdent
                }
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
instance AsSelect (SqlRecord NotNullable a) a where
    select_ = selectRecord
instance AutoType a => AsSelect (SqlExpr NotNullable a) a where
    select_ expr = select_ (expr, auto @a)
instance AutoType a => AsSelect (SqlExpr Nullable a) (Maybe a) where
    select_ expr = select_ (expr, auto @a)
instance AsSelect (SqlExpr NotNullable a, SqlType' x a) a where
    select_ = uncurry selectValue
instance AsSelect (SqlRecord Nullable a) (Maybe a) where
    select_ = selectRecordMaybe
instance AsSelect (SqlExpr Nullable a, SqlType' x a) (Maybe a) where
    select_ = uncurry selectValueMaybe
instance (AsSelect a ar, AsSelect b br) => AsSelect (a :& b) (ar :& br) where
    select_ (a :& b) =
        (:&) <$> select_ a <*> select_ b

infixr 9 `asType`
asType :: SqlExpr n a -> SqlType' x a -> (SqlExpr n a, SqlType' x a)
asType = (,)

selectRecordUnsafe :: SqlRecord n a -> SqlSelect a
selectRecordUnsafe sqlRecord =
    let renderField = renderQualifiedName (sqlRecordBaseIdent sqlRecord)
    in SqlSelect
        { sqlSelectQueryFragments = NE.toList $ fmap renderField $ sqlRecordFields sqlRecord
        , sqlSelectDecoder = sqlRecordDecoder sqlRecord
        }

selectRecord :: SqlRecord NotNullable a -> SqlSelect a
selectRecord = selectRecordUnsafe

selectRecordMaybe :: SqlRecord Nullable a -> SqlSelect (Maybe a)
selectRecordMaybe sqlRecord =
    nullableSqlSelect $ selectRecordUnsafe sqlRecord

selectValueUnsafe :: SqlExpr n a -> SqlType' x a -> SqlSelect a
selectValueUnsafe expr sqlType =
    SqlSelect
        { sqlSelectQueryFragments = [sqlExprFragment expr]
        , sqlSelectDecoder = \case
            val:rest -> fmap (\parsedVal -> (parsedVal,rest)) $ fromSqlType sqlType val
            _        -> Nothing
        }

selectValue :: SqlExpr NotNullable a -> SqlType' x a -> SqlSelect a
selectValue = selectValueUnsafe

selectValueMaybe :: SqlExpr Nullable a -> SqlType' x a -> SqlSelect (Maybe a)
selectValueMaybe expr sqlType =
    nullableSqlSelect $ selectValueUnsafe expr sqlType

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

where_ :: SqlExpr NotNullable Bool -> SqlQuery ()
where_ expr =
    writeQueryData mempty{sqlQueryDataWhereClause = Just [sqlExprFragment expr]}

binOp :: BSB.Builder -> SqlExpr n a -> SqlExpr n b -> SqlExpr n c
binOp op lhs rhs =
    SqlExpr $ \d ->
        sqlExprFragment lhs d <> (" " <> op <> " ", []) <> sqlExprFragment rhs d

infixr 6 ==., /=.
(==.) :: SqlExpr n a -> SqlExpr n a -> SqlExpr n Bool
(==.) = binOp "="
(/=.) :: SqlExpr n a -> SqlExpr n a -> SqlExpr n Bool
(/=.) = binOp "!="

infix 4 `and_`, `or_`
and_ :: SqlExpr n Bool -> SqlExpr n Bool -> SqlExpr n Bool
and_ = binOp "AND"
or_ :: SqlExpr n Bool -> SqlExpr n Bool -> SqlExpr n Bool
or_ = binOp "OR"

isNull_ :: SqlExpr Nullable a -> SqlExpr n Bool
isNull_ expr =
    SqlExpr $ \d ->
        sqlExprFragment expr d <> " IS NULL"

isNotNull_ :: SqlExpr Nullable a -> SqlExpr NotNullable Bool
isNotNull_ expr =
    SqlExpr $ \d ->
        sqlExprFragment expr d <> " IS NOT NULL"

val :: AutoType i => i -> SqlExpr NotNullable i
val x = valOfType x auto

valOfType :: a -> SqlType' a x -> SqlExpr NotNullable a
valOfType x sqlType =
    let v = case toSqlType sqlType x of
                SqlDefault -> SqlNull -- Default should only be used by insert many
                x          -> x
    in SqlExpr (const ("?", [v]))

just_ :: SqlExpr n a -> SqlExpr Nullable a
just_  = veryUnsafeCoerceSqlExpr

nullable_ :: SqlExpr n (Maybe a) -> SqlExpr Nullable a
nullable_ = veryUnsafeCoerceSqlExpr

instance AsFrom (Table i r) (SqlRecord NotNullable r) where
    asFrom = table_

table_ :: Table x r -> From (SqlRecord NotNullable r)
table_ model = From $ do
    ident <- freshIdent (tableName model)
    let originalTableIdent = unsafeIdentFromString (tableName model)
        fromFragment = if ident == originalTableIdent
                          then \d -> (renderIdent d ident, [])
                          else \d -> (renderIdent d originalTableIdent <> " AS " <> renderIdent d ident, [])
        fieldIdent field = unsafeIdentFromString (fieldName field)
        unqualifiedFields = fmap fieldIdent $ tableFields model
    pure ( fromFragment
         , SqlRecord
             { sqlRecordFieldMap = fmap unsafeIdentFromString (tableFieldMap model)
             , sqlRecordDecoder = tableDecoder model
             , sqlRecordBaseIdent = ident
             , sqlRecordFields = NE.fromList unqualifiedFields
             }
         )

instance AsAliasedReference a => AsFrom (SqlQuery a) a where
    asFrom = subquery_

subquery_ :: AsAliasedReference a => SqlQuery a -> From a
subquery_ query = From $ do
    queryState <- SqlQuery get
    SqlQuery $ put $ queryState{ sqsQueryParts = mempty }
    ref <- alias_ =<< query
    subqueryState <- SqlQuery get
    SqlQuery $ put queryState{ sqsIdentifierMap = sqsIdentifierMap subqueryState }
    subqueryIdent <- freshIdent "q"
    let subqueryFragment dialect =
            let (text, values) = renderSelect dialect (aliasedSelectQueryFragment ref) (sqsQueryParts subqueryState)
            in (BSB.byteString text, values)
    pure ( \d -> "(" <> subqueryFragment d <> ")" <> " AS " <> (renderIdent d subqueryIdent, [])
         , aliasedValue ref subqueryIdent
         )

type OnClause a = a -> SqlExpr NotNullable Bool

infix 9 `on_`
on_ :: (OnClause a -> b) -> OnClause a -> b
on_ = ($)

data a :& b = a :& b
    deriving (Show, Eq)
infixl 2 :&

class ToMaybe arg result | arg -> result where
    toMaybe :: arg -> result

instance ToMaybe (SqlExpr n a) (SqlExpr Nullable a) where
    toMaybe = just_
instance ToMaybe (SqlRecord n a) (SqlRecord Nullable a) where
    toMaybe = veryUnsafeCoerceSqlRecord

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

crossJoin_ :: (AsFrom fa a, AsFrom fb b) => fb -> fa -> From (a :& b)
crossJoin_ joinFrom baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    pure ( \d -> baseFromFragment d <> " CROSS JOIN " <> joinFromFragment d
         , baseExpr :& joinExpr
         )

leftJoin_ :: (AsFrom fa a, AsFrom fb b, ToMaybe b mb) => Join fa a fb b (a :& mb)
leftJoin_ = joinHelper "LEFT JOIN" (\a b -> a :& toMaybe b)

infixr 9 ^., ?., ??.
(^.) :: SqlRecord NotNullable rec -> FieldSelector rec o -> SqlExpr NotNullable o
(^.) ent field = projectField ent (fieldSelectorName field)

(?.) :: SqlRecord Nullable rec -> FieldSelector rec o -> SqlExpr Nullable o
(?.) ent field = projectField ent (fieldSelectorName field)

(??.) :: SqlRecord Nullable rec -> FieldSelector rec (Maybe o) -> SqlExpr Nullable o
(??.) ent field = nullable_ $ ent ?. field

data FieldSelector rec a = FieldSelector
    { fieldSelectorName :: String }

instance (KnownSymbol field, HasField field rec res) => IsLabel field (FieldSelector rec res) where
    fromLabel = FieldSelector $ symbolVal (Proxy :: Proxy field)

projectField :: SqlRecord n a -> String -> SqlExpr n' d
projectField ent name =
    let nameIdent = fromMaybe (unsafeIdentFromString (C8.pack name)) $ Map.lookup name (sqlRecordFieldMap ent)
    in SqlExpr $ renderQualifiedName (sqlRecordBaseIdent ent) nameIdent

insertInto :: Backend -> Table w r -> SelectQuery r -> IO Int64
insertInto conn t q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        dialect = connDialect conn
        (qText, qVals) = renderSelect dialect (sqlSelectQueryFragments sqlSelect) qData
        insertText = "INSERT INTO " <> dialectEscapeIdentifier dialect (tableName t) <> " " <> qText
    in connExecute conn insertText qVals
