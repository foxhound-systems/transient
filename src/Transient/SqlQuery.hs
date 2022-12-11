{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
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
import           Transient.Internal.SqlType
import           Transient.Internal.SqlValue

import           Data.Data                     (Data, constrFields,
                                                dataTypeName, dataTypeOf,
                                                indexConstr)
import           Data.Int                      (Int64)
import           Data.Proxy                    (Proxy (..))
import           GHC.OverloadedLabels          (IsLabel (..))
import           GHC.Records                   (HasField (..))
import           GHC.TypeLits                  (KnownSymbol, symbolVal)

type QueryFragment be = SqlDialect -> (BSB.Builder, [SqlValue be])

instance IsString (BSB.Builder, [SqlValue be]) where
    fromString s = (fromString s, [])

data Nullable
data NotNullable
data SqlExpr be nullable a = SqlExpr
    { sqlExprFragment  :: QueryFragment be
    } deriving Functor

veryUnsafeCoerceSqlExpr :: SqlExpr be n a -> SqlExpr be n' a'
veryUnsafeCoerceSqlExpr (SqlExpr f) = SqlExpr f

newtype SqlRecord be nullable a = SqlRecord (SqlRecordInternal be a)

data SqlRecordInternal be a = SqlRecordInternal
    { sqlRecordFieldMap  :: Map String Ident
    , sqlRecordDecoder   :: Decoder be a
    , sqlRecordBaseIdent :: Ident
    , sqlRecordFields    :: NonEmpty Ident
    }

veryUnsafeCoerceSqlRecord :: SqlRecord be n a -> SqlRecord be n' a
veryUnsafeCoerceSqlRecord (SqlRecord (SqlRecordInternal a b c d)) = SqlRecord (SqlRecordInternal a b c d)

renderQualifiedName :: Ident -> Ident -> QueryFragment be
renderQualifiedName baseIdent ident d =
    (renderIdent d baseIdent <> "." <> renderIdent d ident, [])

newtype From be a = From
    { unFrom :: SqlQuery be (QueryFragment be, a) }

class AsFrom be f r | f -> r be where
    asFrom :: f -> From be r
instance AsFrom be (From be a) a where
    asFrom = id

data SqlQueryState be = SqlQueryState
    { sqsQueryParts    :: SqlQueryData be
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

freshIdent :: BS.ByteString -> SqlQuery be Ident
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

data SqlQueryData be = SqlQueryData
    { sqlQueryDataFromClause   :: Maybe [QueryFragment be]
    , sqlQueryDataWhereClause  :: Maybe [QueryFragment be]
    , sqlQueryDataHavingClause :: Maybe [QueryFragment be]
    }

newtype SqlQuery be a = SqlQuery
    { unSqlQuery :: State (SqlQueryState be) a }
    deriving newtype (Functor, Applicative, Monad)

instance Semigroup (SqlQueryData be) where
    (SqlQueryData a b c) <> (SqlQueryData a' b' c') = SqlQueryData (a <> a') (b <> b') (c <> c')
instance Monoid (SqlQueryData be) where
    mempty = SqlQueryData mempty mempty mempty
    mappend = (<>)

data SqlSelect be a = SqlSelect
    { sqlSelectQueryFragments :: [QueryFragment be]
    , sqlSelectDecoder        :: Decoder be a
    } deriving Functor

instance Applicative (SqlSelect be) where
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

data AliasedReference be a = AliasedReference
    { aliasedSelectQueryFragment :: [QueryFragment be]
    , aliasedValue               :: Ident -> a
    } deriving Functor

instance Applicative (AliasedReference be) where
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

class AsAliasedReference be a r | a -> be r where
    alias_ :: a -> SqlQuery be (AliasedReference be r)

instance AsAliasedReference be (AliasedReference be a) a where
    alias_ = pure
instance Data a => AsAliasedReference be (SqlSelect be a) (SqlRecord be NotNullable a) where
    alias_ = mkSqlRecord
instance AsAliasedReference be (SqlExpr be n a) (SqlExpr be n a) where
    alias_ = sqlExprAliasedReference

sqlExprAliasedReference :: SqlExpr be n a -> SqlQuery be (AliasedReference be (SqlExpr be n a))
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

instance AsAliasedReference be (SqlRecord be n a) (SqlRecord be n a) where
    alias_ = sqlRecordAliasedReference

sqlRecordAliasedReference :: SqlRecord be n a -> SqlQuery be (AliasedReference be (SqlRecord be n a))
sqlRecordAliasedReference (SqlRecord record) = do
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
            SqlRecord $ record
                { sqlRecordFields = aliasedFieldIdents
                , sqlRecordFieldMap = aliasedFieldMap
                , sqlRecordBaseIdent = subqueryIdent
                }
        }

instance (AsAliasedReference be a a', AsAliasedReference be b b') => AsAliasedReference be (a :& b) (a' :& b') where
    alias_ (a :& b) = do
        a' <- alias_ a
        b' <- alias_ b
        pure $ (:&) <$> a' <*> b'

maybeDecoder :: SqlBackend be => Int -> Decoder be a -> Decoder be (Maybe a)
maybeDecoder columnCount decoder vals =
    let (columns, rest) = splitAt columnCount vals
    in
    if (all (== nullValue) columns) then
        pure (Nothing, rest)
    else do
        (x, []) <- decoder columns
        pure (Just x, rest)

nullableSqlSelect :: SqlBackend be => SqlSelect be a -> SqlSelect be (Maybe a)
nullableSqlSelect base =
    base{ sqlSelectDecoder = maybeDecoder (length $ sqlSelectQueryFragments base) (sqlSelectDecoder base) }

select :: AsSelect be a r => a -> SelectQuery be r
select = pure . select_

class AsSelect be a r | a -> be r where
    select_ :: a -> SqlSelect be r
instance AsSelect be (SqlSelect be a) a where
    select_ = id
instance AsSelect be (SqlRecord be NotNullable a) a where
    select_ = selectRecord
instance AutoType be a => AsSelect be (SqlExpr be NotNullable a) a where
    select_ expr = select_ (expr, auto @be @a)
instance (SqlBackend be, AutoType be a) => AsSelect be (SqlExpr be Nullable a) (Maybe a) where
    select_ expr = select_ (expr, auto @be @a)
instance AsSelect be (SqlExpr be NotNullable a, SqlType' be x a) a where
    select_ = uncurry selectValue
instance SqlBackend be => AsSelect be (SqlRecord be Nullable a) (Maybe a) where
    select_ = selectRecordMaybe
instance SqlBackend be => AsSelect be (SqlExpr be Nullable a, SqlType' be x a) (Maybe a) where
    select_ = uncurry selectValueMaybe
instance (AsSelect be a ar, AsSelect be b br) => AsSelect be (a :& b) (ar :& br) where
    select_ (a :& b) =
        (:&) <$> select_ a <*> select_ b

infixr 9 `asType`
asType :: SqlExpr be n a -> SqlType' be x a -> (SqlExpr be n a, SqlType' be x a)
asType = (,)

selectRecordUnsafe :: SqlRecord be n a -> SqlSelect be a
selectRecordUnsafe (SqlRecord sqlRecord) =
    let renderField = renderQualifiedName (sqlRecordBaseIdent sqlRecord)
    in SqlSelect
        { sqlSelectQueryFragments = NE.toList $ fmap renderField $ sqlRecordFields sqlRecord
        , sqlSelectDecoder = sqlRecordDecoder sqlRecord
        }

selectRecord :: SqlRecord be NotNullable a -> SqlSelect be a
selectRecord = selectRecordUnsafe

selectRecordMaybe :: SqlBackend be => SqlRecord be Nullable a -> SqlSelect be (Maybe a)
selectRecordMaybe sqlRecord =
    nullableSqlSelect $ selectRecordUnsafe sqlRecord

selectValueUnsafe :: SqlExpr be n a -> SqlType' be x a -> SqlSelect be a
selectValueUnsafe expr sqlType =
    SqlSelect
        { sqlSelectQueryFragments = [sqlExprFragment expr]
        , sqlSelectDecoder = \case
            val:rest -> fmap (\parsedVal -> (parsedVal,rest)) $ fromSqlType sqlType val
            _        -> Nothing
        }

selectValue :: SqlExpr be NotNullable a -> SqlType' be x a -> SqlSelect be a
selectValue = selectValueUnsafe

selectValueMaybe :: SqlBackend be => SqlExpr be Nullable a -> SqlType' be x a -> SqlSelect be (Maybe a)
selectValueMaybe expr sqlType =
    nullableSqlSelect $ selectValueUnsafe expr sqlType

type SelectQuery be r = SqlQuery be (SqlSelect be r)

runSelectUnique :: Backend be -> SelectQuery be r -> IO (Maybe r)
runSelectUnique conn q = fmap listToMaybe $ runSelectMany conn q

unSelectQuery :: SelectQuery be a -> (SqlSelect be a, SqlQueryState be)
unSelectQuery q =
    runState (unSqlQuery q) (SqlQueryState mempty mempty)

runSelectMany :: Backend be -> SelectQuery be r -> IO [r]
runSelectMany conn q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        (qText, qVals) = renderSelect (connDialect conn) (sqlSelectQueryFragments sqlSelect) qData
    in do
        rVals <- connQuery conn qText qVals
        pure $ flip mapMaybe rVals $ \vals -> do
            (v, []) <- sqlSelectDecoder sqlSelect vals
            pure v

uncommas :: [QueryFragment be] -> QueryFragment be
uncommas frags dialect =
    fold $ List.intersperse ", " $
    fmap ($ dialect) frags

renderSelect :: SqlDialect -> [QueryFragment be] -> SqlQueryData be -> (BS.ByteString, [SqlValue be])
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

from :: AsFrom be f r => f -> SqlQuery be r
from f = do
    (fromClause, res) <- unFrom $ asFrom f
    writeQueryData mempty{sqlQueryDataFromClause = Just [fromClause]}
    pure res

writeQueryData :: SqlQueryData be -> SqlQuery be ()
writeQueryData queryData =
    SqlQuery $ do
        sqs <- get
        put $ sqs{ sqsQueryParts = sqsQueryParts sqs <> queryData }

where_ :: SqlExpr be NotNullable Bool -> SqlQuery be ()
where_ expr =
    writeQueryData mempty{sqlQueryDataWhereClause = Just [sqlExprFragment expr]}

binOp :: BSB.Builder -> SqlExpr be n a -> SqlExpr be n b -> SqlExpr be n c
binOp op lhs rhs =
    SqlExpr $ \d ->
        sqlExprFragment lhs d <> (" " <> op <> " ", []) <> sqlExprFragment rhs d

infixr 6 ==., /=.
(==.) :: SqlExpr be n a -> SqlExpr be n a -> SqlExpr be n Bool
(==.) = binOp "="
(/=.) :: SqlExpr be n a -> SqlExpr be n a -> SqlExpr be n Bool
(/=.) = binOp "!="

infix 4 `and_`, `or_`
and_ :: SqlExpr be n Bool -> SqlExpr be n Bool -> SqlExpr be n Bool
and_ = binOp "AND"
or_ :: SqlExpr be n Bool -> SqlExpr be n Bool -> SqlExpr be n Bool
or_ = binOp "OR"

isNull_ :: SqlExpr be Nullable a -> SqlExpr be n Bool
isNull_ expr =
    SqlExpr $ \d ->
        sqlExprFragment expr d <> " IS NULL"

isNotNull_ :: SqlExpr be Nullable a -> SqlExpr be NotNullable Bool
isNotNull_ expr =
    SqlExpr $ \d ->
        sqlExprFragment expr d <> " IS NOT NULL"

val :: AutoType be i => i -> SqlExpr be NotNullable i
val x = valOfType x auto

valOfType :: a -> SqlType' be a x -> SqlExpr be NotNullable a
valOfType x sqlType =
    SqlExpr (const ("?", [toSqlType sqlType QueryContext x]))

just_ :: SqlExpr be n a -> SqlExpr be Nullable a
just_  = veryUnsafeCoerceSqlExpr

nullable_ :: SqlExpr be n (Maybe a) -> SqlExpr be Nullable a
nullable_ = veryUnsafeCoerceSqlExpr

instance AsFrom be (Table be i r) (SqlRecord be NotNullable r) where
    asFrom = table_

table_ :: Table be x r -> From be (SqlRecord be NotNullable r)
table_ model = From $ do
    ident <- freshIdent (tableName model)
    let originalTableIdent = unsafeIdentFromString (tableName model)
        fromFragment = if ident == originalTableIdent
                          then \d -> (renderIdent d ident, [])
                          else \d -> (renderIdent d originalTableIdent <> " AS " <> renderIdent d ident, [])
        fieldIdent field = unsafeIdentFromString (fieldName field)
        unqualifiedFields = fmap fieldIdent $ tableFields model
    pure ( fromFragment
         , SqlRecord $ SqlRecordInternal
             { sqlRecordFieldMap = fmap unsafeIdentFromString (tableFieldMap model)
             , sqlRecordDecoder = tableDecoder model
             , sqlRecordBaseIdent = ident
             , sqlRecordFields = NE.fromList unqualifiedFields
             }
         )

mkSqlRecord :: forall rec be. Data rec => SqlSelect be rec -> SqlQuery be (AliasedReference be (SqlRecord be NotNullable rec))
mkSqlRecord sqlSelect = do
    let recordType = dataTypeOf (undefined :: rec)
    v <- freshIdent (C8.pack $ dataTypeName recordType)
    let recordFields = constrFields $ indexConstr recordType 1
        prefixedFieldName fieldName = v <> "_" <> (unsafeIdentFromString $ C8.pack fieldName)
        aliasedIdents = fmap prefixedFieldName recordFields
        fieldMap = Map.fromList $ zip recordFields aliasedIdents
        renderAliasedSelect fragment aliasIdent =
            \d -> fragment d <> (" AS " <> renderIdent d aliasIdent, [])
    pure $ AliasedReference
        { aliasedSelectQueryFragment =
            fmap (uncurry renderAliasedSelect) $
            zip (sqlSelectQueryFragments sqlSelect) aliasedIdents
        , aliasedValue  = \queryIdent ->
            SqlRecord $
            SqlRecordInternal
                { sqlRecordFields = NE.fromList aliasedIdents
                , sqlRecordFieldMap = fieldMap
                , sqlRecordDecoder = sqlSelectDecoder sqlSelect
                , sqlRecordBaseIdent = queryIdent
                }
        }


instance AsAliasedReference be a a' => AsFrom be (SqlQuery be a) a' where
    asFrom = subquery_ . (alias_ =<<)

subquery_ :: SqlQuery be (AliasedReference be a) -> From be a
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

type OnClause be a = a -> SqlExpr be NotNullable Bool

infix 9 `on_`
on_ :: (OnClause be a -> b) -> OnClause be a -> b
on_ = ($)

data a :& b = a :& b
    deriving (Show, Eq)
infixl 2 :&

class ToMaybe arg result | arg -> result where
    toMaybe :: arg -> result

instance ToMaybe (SqlExpr be n a) (SqlExpr be Nullable a) where
    toMaybe = just_
instance ToMaybe (SqlRecord be n a) (SqlRecord be Nullable a) where
    toMaybe = veryUnsafeCoerceSqlRecord

instance (ToMaybe a a', ToMaybe b b') => ToMaybe (a :& b) (a' :& b') where
    toMaybe (a :& b) = toMaybe a :& toMaybe b

type Join be fa a fb b c = fb -> OnClause be (a :& b) -> fa -> From be c

joinHelper :: (AsFrom be fa a, AsFrom be fb b)
           => BSB.Builder
           -> (a -> b -> c)
           -> Join be fa a fb b c
joinHelper joinType toRes joinFrom onClause baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    let onExpr = onClause (baseExpr :& joinExpr)
    pure ( \d -> baseFromFragment d <> (" " <> joinType <> " ", []) <> joinFromFragment d <> " ON " <> sqlExprFragment onExpr d
         , toRes baseExpr joinExpr
         )

innerJoin_ :: (AsFrom be fa a, AsFrom be fb b) => Join be fa a fb b (a :& b)
innerJoin_ = joinHelper "INNER JOIN" (:&)

crossJoin_ :: (AsFrom be fa a, AsFrom be fb b) => fb -> fa -> From be (a :& b)
crossJoin_ joinFrom baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    pure ( \d -> baseFromFragment d <> " CROSS JOIN " <> joinFromFragment d
         , baseExpr :& joinExpr
         )

leftJoin_ :: (AsFrom be fa a, AsFrom be fb b, ToMaybe b mb) => Join be fa a fb b (a :& mb)
leftJoin_ = joinHelper "LEFT JOIN" (\a b -> a :& toMaybe b)

class ConvertMaybeToNullable a n res n' | a n -> n' res where
instance (a ~ Maybe b, n' ~ Nullable) => ConvertMaybeToNullable a n b n' where
instance {-# OVERLAPPING #-} ConvertMaybeToNullable a n a n where
instance (KnownSymbol field, HasField field rec a, ConvertMaybeToNullable a n res n')
  => HasField field (SqlRecord be n rec) (SqlExpr be n' res) where
    getField rec = projectField rec $ symbolVal (Proxy @field)

infixr 9 ^., ?., ??.
(^.) :: SqlRecord be NotNullable rec -> FieldSelector rec o -> SqlExpr be NotNullable o
(^.) ent field = projectField ent (fieldSelectorName field)

(?.) :: SqlRecord be Nullable rec -> FieldSelector rec o -> SqlExpr be Nullable o
(?.) ent field = projectField ent (fieldSelectorName field)

(??.) :: SqlRecord be Nullable rec -> FieldSelector rec (Maybe o) -> SqlExpr be Nullable o
(??.) ent field = projectField ent (fieldSelectorName field)

data FieldSelector rec a = FieldSelector
    { fieldSelectorName :: String }

instance (KnownSymbol field, HasField field rec res) => IsLabel field (FieldSelector rec res) where
    fromLabel = FieldSelector $ symbolVal (Proxy :: Proxy field)

projectField :: SqlRecord be n a -> String -> SqlExpr be n' d
projectField (SqlRecord ent) name =
    let nameIdent = fromMaybe (unsafeIdentFromString (C8.pack name)) $ Map.lookup name (sqlRecordFieldMap ent)
    in SqlExpr $ renderQualifiedName (sqlRecordBaseIdent ent) nameIdent

insertInto :: Backend be -> Table be w r -> SelectQuery be r -> IO Int64
insertInto conn t q =
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        dialect = connDialect conn
        (qText, qVals) = renderSelect dialect (sqlSelectQueryFragments sqlSelect) qData
        insertText = "INSERT INTO " <> dialectEscapeIdentifier dialect (tableName t) <> " " <> qText
    in connExecute conn insertText qVals
