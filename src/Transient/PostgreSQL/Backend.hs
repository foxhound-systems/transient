{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TypeFamilies          #-}
module Transient.PostgreSQL.Backend
    where

import           Control.Exception                    (bracket, throwIO)
import           Control.Monad                        (join, when)
import qualified Data.ByteString                      as BS
import           Data.Coerce                          (coerce)
import           Data.Either                          (fromRight)
import           Data.Int                             (Int64)
import           Database.PostgreSQL.Simple           as PS
import           Database.PostgreSQL.Simple.FromField as PSFF
import           Database.PostgreSQL.Simple.Internal  as PSI
import           Database.PostgreSQL.Simple.ToField   as PSTF
import           Database.PostgreSQL.Simple.Types     as PST
import           System.IO.Unsafe                     (unsafeDupablePerformIO)
import           Transient.Internal.Backend
import           Transient.Internal.SqlDialect
import           Transient.Internal.SqlType
import           Transient.Internal.SqlValue

import qualified Data.Text                            as T
import           Data.Time.Clock                      (UTCTime)

data Postgres
instance SqlBackend Postgres where
    data SqlValue Postgres
        = SqlString T.Text
        | SqlInt Int64
        | SqlUTCTime UTCTime
        | SqlBool Bool
        | SqlNull
        | SqlDefault
        | SqlUnknown BS.ByteString
        deriving (Eq, Show)

    nullValue = SqlNull

notNullable :: SqlType Postgres (Maybe a) -> SqlType Postgres a
notNullable base =
    base{ sqlTypeConstraints = sqlTypeConstraints base <> ["NOT NULL"]
        , toSqlType = \ctx -> toSqlType base ctx . Just
        , fromSqlType = \case
            SqlNull -> Nothing
            value   -> join $ fromSqlType base value
        }

newtype DefaultExpression a = DefaultExpression
    { unDefaultExpression :: BS.ByteString }

currentTimestamp :: DefaultExpression UTCTime
currentTimestamp = DefaultExpression "CURRENT_TIMESTAMP"

defaultTo :: DefaultExpression a -> SqlType Postgres (Maybe a) -> SqlType' Postgres (Maybe a) a
defaultTo defaultExpression base =
    base{ sqlTypeConstraints = sqlTypeConstraints base <> [" DEFAULT " <> unDefaultExpression defaultExpression]
        , toSqlType = \ctx -> \case
            Just v  ->
                toSqlType base ctx (Just v)
            Nothing ->
                case ctx of
                  QueryContext   -> SqlNull
                  CommandContext -> SqlDefault
        , fromSqlType = fromSqlType (notNullable base)
        }

serial :: SqlType' Postgres (Maybe Int64) Int64
serial =
    SqlType
        { sqlType = "SERIAL"
        , sqlTypeConstraints = []
        , toSqlType = \ctx -> \case
            Just i  -> SqlInt i
            Nothing ->
                case ctx of
                  QueryContext   -> SqlNull
                  CommandContext -> SqlDefault
        , fromSqlType = \case
            SqlInt value -> Just value
            _            -> Nothing
        }

nullableSqlType :: BS.ByteString -> (a -> SqlValue Postgres) -> (SqlValue Postgres -> Maybe a) -> SqlType Postgres (Maybe a)
nullableSqlType typeName to from =
    SqlType
        { sqlType = typeName
        , sqlTypeConstraints = []
        , toSqlType = \ctx -> \case
            Just v  ->
                to v
            Nothing ->
                case ctx of
                  QueryContext   -> SqlNull
                  CommandContext -> SqlDefault
        , fromSqlType = \case
            SqlNull -> Just Nothing
            value   -> fmap Just (from value)
        }

timestamptz :: SqlType Postgres (Maybe UTCTime)
timestamptz =
    nullableSqlType "timestamptz"
                    SqlUTCTime
                    (\case
                        SqlUTCTime t -> Just t
                        _            -> Nothing
                    )

instance AutoType Postgres Int64 where
    auto = notNullable bigInt
instance AutoType Postgres (Maybe Int64) where
    auto = bigInt

bigInt :: SqlType Postgres (Maybe Int64)
bigInt =
    nullableSqlType "BIGINT"
                    SqlInt
                    (\case
                        SqlInt i -> Just i
                        _        -> Nothing
                    )

instance AutoType Postgres T.Text where
    auto = notNullable text
instance AutoType Postgres (Maybe T.Text) where
    auto = text

text :: SqlType Postgres (Maybe T.Text)
text =
    nullableSqlType "TEXT"
                    SqlString
                    (\case
                        SqlString s -> Just s
                        _           -> Nothing
                    )

instance AutoType Postgres Bool where
    auto = notNullable bool
instance AutoType Postgres (Maybe Bool) where
    auto = bool

bool :: SqlType Postgres (Maybe Bool)
bool =
    nullableSqlType
        "BOOL"
        SqlBool
        (\case
            SqlBool b -> Just b
            _         -> Nothing
        )

withBackend :: ConnectInfo -> (Backend Postgres -> IO a) -> IO a
withBackend connInfo k =
    bracket
        (mkBackend <$> connect connInfo)
        (connClose)
        k


mkBackend :: PS.Connection -> Backend Postgres
mkBackend connection =
    Backend
        { connTag     = "postgresql"
        , connDialect = mkDialect connection
        , connQuery   = runQuery connection
        , connExecute = runExecute connection
        , connClose   = close connection
        }

mkDialect :: PS.Connection -> SqlDialect
mkDialect conn =
    SqlDialect
        { dialectEscapeIdentifier = \ident ->
            case unsafeDupablePerformIO $ escapeIdentifier conn ident of
              Right r -> r
        }

runQuery :: PS.Connection -> BS.ByteString -> [SqlValue Postgres] -> IO [[SqlValue Postgres]]
runQuery conn q vals = do
    print q
    when (not $ null vals) $
        print vals
    query conn (Query q) (fmap valToField vals)

runExecute :: PS.Connection -> BS.ByteString -> [SqlValue Postgres] -> IO Int64
runExecute conn q vals = do
    print q
    when (not $ null vals) $
        print vals
    execute conn (Query q) (fmap valToField vals)

instance FromField (SqlValue Postgres) where
    fromField = valFromField

valFromField :: FieldParser (SqlValue Postgres)
valFromField f mv = do
    typ <- typename f
    case mv of
      Nothing ->
          pure SqlNull
      Just v ->
        case typ of
          "bool"        -> SqlBool <$> fromField f mv
          "int2"        -> SqlInt <$> fromField f mv
          "int4"        -> SqlInt <$> fromField f mv
          "int8"        -> SqlInt <$> fromField f mv
          "timestamp"   -> SqlUTCTime <$> fromField f mv
          "timestamptz" -> SqlUTCTime <$> fromField f mv
          "bpchar"      -> SqlString <$> fromField f mv
          "varchar"     -> SqlString <$> fromField f mv
          "text"        -> SqlString <$> fromField f mv
          _             -> pure $ SqlUnknown v

valToField :: SqlValue Postgres -> Action
valToField (SqlInt i)     = toField i
valToField (SqlString s)  = toField s
valToField (SqlUTCTime t) = toField t
valToField (SqlBool b)    = toField b
valToField SqlNull        = Plain "NULL"
valToField SqlDefault     = Plain "DEFAULT"
