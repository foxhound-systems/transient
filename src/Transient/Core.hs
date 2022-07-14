{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module Transient.Core
    ( SqlValue(..)
    , SqlType'(..)
    , SqlType
    , module Transient.Core
    )
    where

import           Control.Monad               (join)
import qualified Data.ByteString             as BS
import           Data.Int                    (Int64)
import qualified Data.List                   as List
import           Data.String                 (IsString (..))
import qualified Data.Text                   as T
import           Data.Time.Clock             (UTCTime)
import           Data.Void                   (Void, absurd)

import           Transient.Internal.Backend
import           Transient.Internal.SqlType
import           Transient.Internal.SqlValue

class AutoType a where
    auto :: SqlType a

notNullable :: SqlType (Maybe a) -> SqlType a
notNullable base =
    base{ sqlTypeConstraints = sqlTypeConstraints base <> ["NOT NULL"]
        , toSqlType = toSqlType base . Just
        , fromSqlType = \case
            SqlNull -> Nothing
            value   -> join $ fromSqlType base value
        }

newtype DefaultExpression a = DefaultExpression
    { unDefaultExpression :: BS.ByteString }

currentTimestamp :: DefaultExpression UTCTime
currentTimestamp = DefaultExpression "CURRENT_TIMESTAMP"

defaultTo :: DefaultExpression a -> SqlType (Maybe a) -> SqlType' (Maybe a) a
defaultTo defaultExpression base =
    base{ sqlTypeConstraints = sqlTypeConstraints base <> [" DEFAULT " <> unDefaultExpression defaultExpression]
        , fromSqlType = fromSqlType (notNullable base)
        }

serial :: SqlType' (Maybe Int64) Int64
serial =
    SqlType
        { sqlType = "SERIAL"
        , sqlTypeConstraints = []
        , toSqlType = \case
            Just i  -> SqlInt i
            Nothing -> SqlDefault
        , fromSqlType = \case
            SqlInt value -> Just value
            _            -> Nothing
        }

nullableSqlType :: BS.ByteString -> (a -> SqlValue) -> (SqlValue -> Maybe a) -> SqlType (Maybe a)
nullableSqlType typeName to from =
    SqlType
        { sqlType = typeName
        , sqlTypeConstraints = []
        , toSqlType = \case
            Just x  -> to x
            Nothing -> SqlDefault
        , fromSqlType = \case
            SqlNull -> Just Nothing
            value   -> fmap Just (from value)
        }

timestamptz :: SqlType (Maybe UTCTime)
timestamptz =
    nullableSqlType "timestamptz"
                    SqlUTCTime
                    (\case
                        SqlUTCTime t -> Just t
                        _            -> Nothing
                    )

instance AutoType Int64 where
    auto = notNullable bigInt
instance AutoType (Maybe Int64) where
    auto = bigInt

bigInt :: SqlType (Maybe Int64)
bigInt =
    nullableSqlType "BIGINT"
                    SqlInt
                    (\case
                        SqlInt i -> Just i
                        _        -> Nothing
                    )

instance AutoType T.Text where
    auto = notNullable text
instance AutoType (Maybe T.Text) where
    auto = text

text :: SqlType (Maybe T.Text)
text =
    nullableSqlType "TEXT"
                    SqlString
                    (\case
                        SqlString s -> Just s
                        _           -> Nothing
                    )

instance AutoType Bool where
    auto = notNullable bool
instance AutoType (Maybe Bool) where
    auto = bool

bool :: SqlType (Maybe Bool)
bool =
    nullableSqlType
        "BOOL"
        SqlBool
        (\case
            SqlBool b -> Just b
            _         -> Nothing
        )

type FromRowFn a = [SqlValue] -> Maybe (a, [SqlValue])

fieldParser :: SqlType' x a -> FromRowFn a
fieldParser sqlType vs = do
    let vHead:vTail = vs
    r <- fromSqlType sqlType vHead
    pure (r, vTail)

type ToRowFn a = a -> [SqlValue]

data FieldDef = FieldDef
    { fieldName        :: BS.ByteString
    , fieldType        :: BS.ByteString
    , fieldConstraints :: [BS.ByteString]
    } deriving Show

data Table i o = Table
    { tableToRow   :: ToRowFn i
    , tableFromRow :: FromRowFn o
    , tableName    :: BS.ByteString
    , tableFields  :: [FieldDef]
    }

data Fields i o = Fields
    { fieldsToRow     :: ToRowFn i
    , fieldsFromRow   :: FromRowFn o
    , fieldsFieldDefs :: [FieldDef]
    } deriving Functor

instance Applicative (Fields i) where
    pure a = Fields
        { fieldsFromRow = \vs -> Just (a, vs)
        , fieldsToRow = \_ -> []
        , fieldsFieldDefs = []
        }

    a <*> b = Fields
        { fieldsFromRow = \vals -> do
            (f, vals2) <- fieldsFromRow a vals
            (a, rest) <- fieldsFromRow b vals2
            pure (f a, rest)
        , fieldsToRow = fieldsToRow a <> fieldsToRow b
        , fieldsFieldDefs = fieldsFieldDefs a <> fieldsFieldDefs b
        }

table :: BS.ByteString -> Fields i o -> Table i o
table tableName fields =
    Table
        { tableToRow = fieldsToRow fields
        , tableFromRow = fieldsFromRow fields
        , tableFields = fieldsFieldDefs fields
        , tableName = tableName
        }


data EntityField ent i o = EntityField
    { entityFieldName :: BS.ByteString
    , entityFieldType :: SqlType' i o
    }

type SimpleField rec a = EntityField rec a a
instance AutoType t => IsString (EntityField r t t) where
    fromString s = EntityField (fromString s) auto

instance IsString (SqlType' i o -> EntityField r i o) where
    fromString s = EntityField (fromString s)

fieldDef :: EntityField x i o -> FieldDef
fieldDef (EntityField fieldName t) =
    FieldDef fieldName (sqlType t) (sqlTypeConstraints t)

field :: (rec -> i) -> EntityField x i o -> Fields rec o
field fromRec f@(EntityField _ t) =
    Fields
        { fieldsFieldDefs = [fieldDef f]
        , fieldsFromRow = fieldParser t
        , fieldsToRow = List.singleton . toSqlType t . fromRec
        }

createTable :: Backend -> Table i o -> IO Int64
createTable c t =
    let renderField f =
            fieldName f <> " " <> fieldType f <> " " <> BS.intercalate " " (fieldConstraints f)
        stmt = "CREATE TABLE " <> tableName t <> "("
                    <> BS.intercalate ", " (fmap renderField (tableFields t))
                    <> ")"
    in connExecute c stmt []

insertMany :: Backend -> Table i x -> [i] -> IO Int64
insertMany c t vals =
    let commas = BS.intercalate ", "
        fieldsPlaceholder fieldCount =
            "(" <> commas (replicate fieldCount "?") <> ")"
        fieldValues fieldCount =
            " VALUES " <> commas (replicate (length vals) (fieldsPlaceholder fieldCount))
    in connExecute c
        ("INSERT INTO " <> tableName t
                   <> " (" <> commas (fmap fieldName (tableFields t)) <> ")"
                   <> fieldValues (length $ tableFields t)) (join $ fmap (tableToRow t) vals)

