{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE FunctionalDependencies     #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

module Transient.SqlQuery
    where

import           Control.Monad                 (join)
import           Control.Monad.Reader          (ReaderT, ask, liftIO)
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

import           Data.Data                     (Data, constrFields,
                                                dataTypeName, dataTypeOf,
                                                indexConstr)
import           Data.Int                      (Int64)
import           Data.Proxy                    (Proxy (..))
import           GHC.OverloadedLabels          (IsLabel (..))
import           GHC.Records                   (HasField (..))
import           GHC.TypeLits                  (KnownSymbol, symbolVal)

type QueryFragment sqlValue = SqlDialect -> (BSB.Builder, [sqlValue])

instance IsString (BSB.Builder, [sqlValue]) where
    fromString s = (fromString s, [])

data Nullable
data NotNullable
data SqlExpr sqlValue nullable a = SqlExpr
    { sqlExprFragment  :: QueryFragment sqlValue
    } deriving Functor

veryUnsafeCoerceSqlExpr :: SqlExpr sqlValue n a -> SqlExpr sqlValue n' a'
veryUnsafeCoerceSqlExpr (SqlExpr f) = SqlExpr f

newtype SqlRecord sqlValue nullable a = SqlRecord (SqlRecordInternal sqlValue a)

data SqlRecordInternal sqlValue a = SqlRecordInternal
    { sqlRecordFieldMap  :: Map String Ident
    , sqlRecordDecoder   :: Decoder sqlValue a
    , sqlRecordBaseIdent :: Ident
    , sqlRecordFields    :: NonEmpty Ident
    }

veryUnsafeCoerceSqlRecord :: SqlRecord sqlValue n a -> SqlRecord sqlValue n' a
veryUnsafeCoerceSqlRecord (SqlRecord (SqlRecordInternal a b c d)) = SqlRecord (SqlRecordInternal a b c d)

renderQualifiedName :: Ident -> Ident -> QueryFragment sqlValue
renderQualifiedName baseIdent ident d =
    (renderIdent d baseIdent <> "." <> renderIdent d ident, [])

newtype From sqlValue a = From
    { unFrom :: SqlQuery sqlValue (QueryFragment sqlValue, a) }

class AsFrom sqlValue f r | f -> r sqlValue where
    asFrom :: f -> From sqlValue r
instance AsFrom sqlValue (From sqlValue a) a where
    asFrom = id

data SqlQueryState sqlValue = SqlQueryState
    { sqsQueryParts    :: SqlQueryData sqlValue
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

freshIdent :: BS.ByteString -> SqlQuery sqlValue Ident
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

data SqlQueryData sqlValue = SqlQueryData
    { sqlQueryDataFromClause   :: Maybe [QueryFragment sqlValue]
    , sqlQueryDataWhereClause  :: Maybe [QueryFragment sqlValue]
    , sqlQueryDataHavingClause :: Maybe [QueryFragment sqlValue]
    }

newtype SqlQuery sqlValue a = SqlQuery
    { unSqlQuery :: State (SqlQueryState sqlValue) a }
    deriving newtype (Functor, Applicative, Monad)

instance Semigroup (SqlQueryData sqlValue) where
    (SqlQueryData a b c) <> (SqlQueryData a' b' c') = SqlQueryData (a <> a') (b <> b') (c <> c')
instance Monoid (SqlQueryData sqlValue) where
    mempty = SqlQueryData mempty mempty mempty
    mappend = (<>)

data SqlSelect sqlValue a = SqlSelect
    { sqlSelectQueryFragments :: [QueryFragment sqlValue]
    , sqlSelectDecoder        :: Decoder sqlValue a
    } deriving Functor

instance Applicative (SqlSelect sqlValue) where
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

data AliasedReference sqlValue a = AliasedReference
    { aliasedSelectQueryFragment :: [QueryFragment sqlValue]
    , aliasedValue               :: Ident -> a
    } deriving Functor

instance Applicative (AliasedReference sqlValue) where
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

class AsAliasedReference sqlValue a r | a -> sqlValue r where
    alias_ :: a -> SqlQuery sqlValue (AliasedReference sqlValue r)

instance AsAliasedReference sqlValue (AliasedReference sqlValue a) a where
    alias_ = pure
instance Data a => AsAliasedReference sqlValue (SqlSelect sqlValue a) (SqlRecord sqlValue NotNullable a) where
    alias_ = mkSqlRecord
instance AsAliasedReference sqlValue (SqlExpr sqlValue n a) (SqlExpr sqlValue n a) where
    alias_ = sqlExprAliasedReference

sqlExprAliasedReference :: SqlExpr sqlValue n a -> SqlQuery sqlValue (AliasedReference sqlValue (SqlExpr sqlValue n a))
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

instance AsAliasedReference sqlValue (SqlRecord sqlValue n a) (SqlRecord sqlValue n a) where
    alias_ = sqlRecordAliasedReference

sqlRecordAliasedReference :: SqlRecord sqlValue n a -> SqlQuery sqlValue (AliasedReference sqlValue (SqlRecord sqlValue n a))
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

instance (AsAliasedReference sqlValue a a', AsAliasedReference sqlValue b b') => AsAliasedReference sqlValue (a :& b) (a' :& b') where
    alias_ (a :& b) = do
        a' <- alias_ a
        b' <- alias_ b
        pure $ (:&) <$> a' <*> b'

maybeDecoder :: SqlBackend sqlValue => Int -> Decoder sqlValue a -> Decoder sqlValue (Maybe a)
maybeDecoder columnCount decoder vals =
    let (columns, rest) = splitAt columnCount vals
    in
    if all isNullValue columns then
        pure (Nothing, rest)
    else do
        (x, []) <- decoder columns
        pure (Just x, rest)

nullableSqlSelect :: SqlBackend sqlValue => SqlSelect sqlValue a -> SqlSelect sqlValue (Maybe a)
nullableSqlSelect base =
    base{ sqlSelectDecoder = maybeDecoder (length $ sqlSelectQueryFragments base) (sqlSelectDecoder base) }

select :: AsSelect sqlValue a r => a -> SelectQuery sqlValue r
select = pure . select_

class AsSelect sqlValue a r | a -> sqlValue r where
    select_ :: a -> SqlSelect sqlValue r
instance AsSelect sqlValue (SqlSelect sqlValue a) a where
    select_ = id
instance AsSelect sqlValue (SqlRecord sqlValue NotNullable a) a where
    select_ = selectRecord
instance AutoType sqlValue a => AsSelect sqlValue (SqlExpr sqlValue NotNullable a) a where
    select_ expr = select_ (expr, auto @sqlValue @a)
instance (SqlBackend sqlValue, AutoType sqlValue a) => AsSelect sqlValue (SqlExpr sqlValue Nullable a) (Maybe a) where
    select_ expr = select_ (expr, auto @sqlValue @a)
instance AsSelect sqlValue (SqlExpr sqlValue NotNullable a, SqlType' sqlValue x a) a where
    select_ = uncurry selectValue
instance SqlBackend sqlValue => AsSelect sqlValue (SqlRecord sqlValue Nullable a) (Maybe a) where
    select_ = selectRecordMaybe
instance SqlBackend sqlValue => AsSelect sqlValue (SqlExpr sqlValue Nullable a, SqlType' sqlValue x a) (Maybe a) where
    select_ = uncurry selectValueMaybe
instance (AsSelect sqlValue a ar, AsSelect sqlValue b br) => AsSelect sqlValue (a :& b) (ar :& br) where
    select_ (a :& b) =
        (:&) <$> select_ a <*> select_ b

infixr 9 `asType`
asType :: SqlExpr sqlValue n a -> SqlType' sqlValue x a -> (SqlExpr sqlValue n a, SqlType' sqlValue x a)
asType = (,)

selectRecordUnsafe :: SqlRecord sqlValue n a -> SqlSelect sqlValue a
selectRecordUnsafe (SqlRecord sqlRecord) =
    let renderField = renderQualifiedName (sqlRecordBaseIdent sqlRecord)
    in SqlSelect
        { sqlSelectQueryFragments = NE.toList $ fmap renderField $ sqlRecordFields sqlRecord
        , sqlSelectDecoder = sqlRecordDecoder sqlRecord
        }

selectRecord :: SqlRecord sqlValue NotNullable a -> SqlSelect sqlValue a
selectRecord = selectRecordUnsafe

selectRecordMaybe :: SqlBackend sqlValue => SqlRecord sqlValue Nullable a -> SqlSelect sqlValue (Maybe a)
selectRecordMaybe sqlRecord =
    nullableSqlSelect $ selectRecordUnsafe sqlRecord

selectValueUnsafe :: SqlExpr sqlValue n a -> SqlType' sqlValue x a -> SqlSelect sqlValue a
selectValueUnsafe expr sqlType =
    SqlSelect
        { sqlSelectQueryFragments = [sqlExprFragment expr]
        , sqlSelectDecoder = \case
            val:rest -> fmap (\parsedVal -> (parsedVal,rest)) $ fromSqlType sqlType val
            _        -> Nothing
        }

selectValue :: SqlExpr sqlValue NotNullable a -> SqlType' sqlValue x a -> SqlSelect sqlValue a
selectValue = selectValueUnsafe

selectValueMaybe :: SqlBackend sqlValue => SqlExpr sqlValue Nullable a -> SqlType' sqlValue x a -> SqlSelect sqlValue (Maybe a)
selectValueMaybe expr sqlType =
    nullableSqlSelect $ selectValueUnsafe expr sqlType

type SelectQuery sqlValue r = SqlQuery sqlValue (SqlSelect sqlValue r)

runSelectUnique :: SelectQuery sqlValue r -> ReaderT (Backend sqlValue) IO (Maybe r)
runSelectUnique = fmap listToMaybe . runSelectMany

unSelectQuery :: SelectQuery sqlValue a -> (SqlSelect sqlValue a, SqlQueryState sqlValue)
unSelectQuery q =
    runState (unSqlQuery q) (SqlQueryState mempty mempty)

runSelectMany :: SelectQuery sqlValue r -> ReaderT (Backend sqlValue) IO [r]
runSelectMany q = do
    conn <- ask
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        (qText, qVals) = renderSelect (connDialect conn) (sqlSelectQueryFragments sqlSelect) qData
    rVals <- liftIO $ connQuery conn qText qVals
    pure $ flip mapMaybe rVals $ \vals -> do
        (v, []) <- sqlSelectDecoder sqlSelect vals
        pure v

uncommas :: [QueryFragment sqlValue] -> QueryFragment sqlValue
uncommas frags dialect =
    fold $ List.intersperse ", " $
    fmap ($ dialect) frags

renderSelect :: SqlDialect -> [QueryFragment sqlValue] -> SqlQueryData sqlValue -> (BS.ByteString, [sqlValue])
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

from :: AsFrom sqlValue f r => f -> SqlQuery sqlValue r
from f = do
    (fromClause, res) <- unFrom $ asFrom f
    writeQueryData mempty{sqlQueryDataFromClause = Just [fromClause]}
    pure res

writeQueryData :: SqlQueryData sqlValue -> SqlQuery sqlValue ()
writeQueryData queryData =
    SqlQuery $ do
        sqs <- get
        put $ sqs{ sqsQueryParts = sqsQueryParts sqs <> queryData }

where_ :: SqlExpr sqlValue NotNullable Bool -> SqlQuery sqlValue ()
where_ expr =
    writeQueryData mempty{sqlQueryDataWhereClause = Just [sqlExprFragment expr]}

binOp :: BSB.Builder -> SqlExpr sqlValue n a -> SqlExpr sqlValue n b -> SqlExpr sqlValue n c
binOp op lhs rhs =
    SqlExpr $ \d ->
        sqlExprFragment lhs d <> (" " <> op <> " ", []) <> sqlExprFragment rhs d

infixr 6 ==., /=.
(==.) :: SqlExpr sqlValue n a -> SqlExpr sqlValue n a -> SqlExpr sqlValue n Bool
(==.) = binOp "="
(/=.) :: SqlExpr sqlValue n a -> SqlExpr sqlValue n a -> SqlExpr sqlValue n Bool
(/=.) = binOp "!="

infix 4 `and_`, `or_`
and_ :: SqlExpr sqlValue n Bool -> SqlExpr sqlValue n Bool -> SqlExpr sqlValue n Bool
and_ = binOp "AND"
or_ :: SqlExpr sqlValue n Bool -> SqlExpr sqlValue n Bool -> SqlExpr sqlValue n Bool
or_ = binOp "OR"

isNull_ :: SqlExpr sqlValue Nullable a -> SqlExpr sqlValue n Bool
isNull_ expr =
    SqlExpr $ \d ->
        sqlExprFragment expr d <> " IS NULL"

isNotNull_ :: SqlExpr sqlValue Nullable a -> SqlExpr sqlValue NotNullable Bool
isNotNull_ expr =
    SqlExpr $ \d ->
        sqlExprFragment expr d <> " IS NOT NULL"

val :: AutoType sqlValue i => i -> SqlExpr sqlValue NotNullable i
val x = valOfType x auto

valOfType :: a -> SqlType' sqlValue a x -> SqlExpr sqlValue NotNullable a
valOfType x sqlType =
    SqlExpr (const ("?", [toSqlType sqlType QueryContext x]))

just_ :: SqlExpr sqlValue n a -> SqlExpr sqlValue Nullable a
just_  = veryUnsafeCoerceSqlExpr

nullable_ :: SqlExpr sqlValue n (Maybe a) -> SqlExpr sqlValue Nullable a
nullable_ = veryUnsafeCoerceSqlExpr

instance AsFrom sqlValue (Table sqlValue i r) (SqlRecord sqlValue NotNullable r) where
    asFrom = table_

table_ :: Table sqlValue x r -> From sqlValue (SqlRecord sqlValue NotNullable r)
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

mkSqlRecord :: forall rec sqlValue. Data rec => SqlSelect sqlValue rec -> SqlQuery sqlValue (AliasedReference sqlValue (SqlRecord sqlValue NotNullable rec))
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


instance AsAliasedReference sqlValue a a' => AsFrom sqlValue (SqlQuery sqlValue a) a' where
    asFrom = subquery_ . (alias_ =<<)

subquery_ :: SqlQuery sqlValue (AliasedReference sqlValue a) -> From sqlValue a
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

type OnClause sqlValue a = a -> SqlExpr sqlValue NotNullable Bool

infix 9 `on_`
on_ :: (OnClause sqlValue a -> b) -> OnClause sqlValue a -> b
on_ = ($)

data a :& b = a :& b
    deriving (Show, Eq)
infixl 2 :&

class ToMaybe arg result | arg -> result where
    toMaybe :: arg -> result

instance ToMaybe (SqlExpr sqlValue n a) (SqlExpr sqlValue Nullable a) where
    toMaybe = just_
instance ToMaybe (SqlRecord sqlValue n a) (SqlRecord sqlValue Nullable a) where
    toMaybe = veryUnsafeCoerceSqlRecord

instance (ToMaybe a a', ToMaybe b b') => ToMaybe (a :& b) (a' :& b') where
    toMaybe (a :& b) = toMaybe a :& toMaybe b

type Join sqlValue fa a fb b c = fb -> OnClause sqlValue (a :& b) -> fa -> From sqlValue c

joinHelper :: (AsFrom sqlValue fa a, AsFrom sqlValue fb b)
           => BSB.Builder
           -> (a -> b -> c)
           -> Join sqlValue fa a fb b c
joinHelper joinType toRes joinFrom onClause baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    let onExpr = onClause (baseExpr :& joinExpr)
    pure ( \d -> baseFromFragment d <> (" " <> joinType <> " ", []) <> joinFromFragment d <> " ON " <> sqlExprFragment onExpr d
         , toRes baseExpr joinExpr
         )

innerJoin_ :: (AsFrom sqlValue fa a, AsFrom sqlValue fb b) => Join sqlValue fa a fb b (a :& b)
innerJoin_ = joinHelper "INNER JOIN" (:&)

crossJoin_ :: (AsFrom sqlValue fa a, AsFrom sqlValue fb b) => fb -> fa -> From sqlValue (a :& b)
crossJoin_ joinFrom baseFrom = From do
    (baseFromFragment, baseExpr) <- unFrom $ asFrom baseFrom
    (joinFromFragment, joinExpr) <- unFrom $ asFrom joinFrom
    pure ( \d -> baseFromFragment d <> " CROSS JOIN " <> joinFromFragment d
         , baseExpr :& joinExpr
         )

leftJoin_ :: (AsFrom sqlValue fa a, AsFrom sqlValue fb b, ToMaybe b mb) => Join sqlValue fa a fb b (a :& mb)
leftJoin_ = joinHelper "LEFT JOIN" (\a b -> a :& toMaybe b)

class ConvertMaybeToNullable a n res n' | a n -> n' res where
instance (a ~ Maybe b, n' ~ Nullable) => ConvertMaybeToNullable a n b n' where
instance {-# OVERLAPPING #-} ConvertMaybeToNullable a n a n where
instance (KnownSymbol field, HasField field rec a, ConvertMaybeToNullable a n res n')
  => HasField field (SqlRecord sqlValue n rec) (SqlExpr sqlValue n' res) where
    getField rec = projectField rec $ symbolVal (Proxy @field)

infixr 9 ^., ?., ??.
(^.) :: SqlRecord sqlValue NotNullable rec -> FieldSelector rec o -> SqlExpr sqlValue NotNullable o
(^.) ent field = projectField ent (fieldSelectorName field)

(?.) :: SqlRecord sqlValue Nullable rec -> FieldSelector rec o -> SqlExpr sqlValue Nullable o
(?.) ent field = projectField ent (fieldSelectorName field)

(??.) :: SqlRecord sqlValue Nullable rec -> FieldSelector rec (Maybe o) -> SqlExpr sqlValue Nullable o
(??.) ent field = projectField ent (fieldSelectorName field)

data FieldSelector rec a = FieldSelector
    { fieldSelectorName :: String }

instance (KnownSymbol field, HasField field rec res) => IsLabel field (FieldSelector rec res) where
    fromLabel = FieldSelector $ symbolVal (Proxy :: Proxy field)

projectField :: SqlRecord sqlValue n a -> String -> SqlExpr sqlValue n' d
projectField (SqlRecord ent) name =
    let nameIdent = fromMaybe (unsafeIdentFromString (C8.pack name)) $ Map.lookup name (sqlRecordFieldMap ent)
    in SqlExpr $ renderQualifiedName (sqlRecordBaseIdent ent) nameIdent

_insertInto :: Table sqlValue w r -> SelectQuery sqlValue r -> ReaderT (Backend sqlValue) IO Int64
_insertInto t q = do
    conn <- ask
    let (sqlSelect, SqlQueryState qData _) = unSelectQuery q
        dialect = connDialect conn
        (qText, qVals) = renderSelect dialect (sqlSelectQueryFragments sqlSelect) qData
        insertText = "INSERT INTO " <> dialectEscapeIdentifier dialect (tableName t) <> " " <> qText
    liftIO $ connExecute conn insertText qVals
