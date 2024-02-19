{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE KindSignatures        #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeApplications      #-}
module Transient.PostgreSQL.Backend
    ( PgValue
    , withBackend
    , mkBackend
    , defaultTo
    , nullable
    , serial
    , bigInt
    , text
    , bool
    , timestamptz
    )
    where

import           Control.Exception                    (bracket, throwIO)
import           Control.Monad                        (join, unless)
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
import           Type.Reflection

import           Data.ByteString                      (ByteString)
import qualified Data.Text                            as T
import           Data.Time.Clock                      (UTCTime)

cast :: forall a b. (Typeable a, Typeable b) => a -> Maybe b
cast a =
    case eqTypeRep (typeRep @a) (typeRep @b) of
      Just HRefl -> Just a
      Nothing    -> Nothing

data PgValue where
    SqlNull :: PgValue
    SqlDefault :: PgValue
    SqlField :: (Show f, FromField f, ToField f) => TypeRep (f :: *) -> f -> PgValue

deriving instance Show PgValue

instance SqlBackend PgValue where
    isNullValue SqlNull = True
    isNullValue _       = False

instance AutoType PgValue a => AutoType PgValue (Maybe a) where
    auto = nullable auto
nullable :: SqlType PgValue a -> SqlType PgValue (Maybe a)
nullable base =
    base{ sqlTypeConstraints = []
        , sqlTypeIsNullable = True
        , toSqlType = maybe SqlNull . toSqlType base
        , fromSqlType = \case
            SqlNull -> Nothing
            value   -> Just <$> fromSqlType base value
        }

newtype DefaultExpression a = DefaultExpression
    { unDefaultExpression :: BS.ByteString }

currentTimestamp :: DefaultExpression UTCTime
currentTimestamp = DefaultExpression "CURRENT_TIMESTAMP"

defaultTo :: DefaultExpression a -> SqlType PgValue a -> SqlType' PgValue (Maybe a) a
defaultTo defaultExpression base =
    base{ sqlTypeConstraints = sqlTypeConstraints base <> [" DEFAULT " <> unDefaultExpression defaultExpression]
        , sqlTypeIsNullable = False
        , toSqlType = \ctx -> \case
            Just v  ->
                toSqlType base ctx v
            Nothing ->
                case ctx of
                  QueryContext   -> SqlNull
                  CommandContext -> SqlDefault
        , fromSqlType = fromSqlType base
        }

serial :: SqlType' PgValue (Maybe Int64) Int64
serial =
    SqlType
        { sqlType = "SERIAL"
        , sqlTypeConstraints = []
        , sqlTypeIsNullable = False
        , toSqlType = \ctx -> \case
            Just i  -> SqlField typeRep i
            Nothing ->
                case ctx of
                  QueryContext   -> SqlNull
                  CommandContext -> SqlDefault
        , fromSqlType = fromSqlField
        }

fromSqlField :: (Typeable a, FromField a) => PgValue -> Maybe a
fromSqlField (SqlField rep value) = withTypeable rep (cast value)
fromSqlField _                    = Nothing

mkSqlType :: (Show a, Typeable a, FromField a, ToField a) => BS.ByteString -> SqlType PgValue a
mkSqlType typeName =
    SqlType
        { sqlType = typeName
        , sqlTypeConstraints = []
        , sqlTypeIsNullable = False
        , toSqlType = \_ -> SqlField typeRep
        , fromSqlType = fromSqlField
        }

instance AutoType PgValue UTCTime where
    auto = timestamptz
timestamptz :: SqlType PgValue UTCTime
timestamptz = mkSqlType "timestamptz"

instance AutoType PgValue Int64 where
    auto = bigInt
bigInt :: SqlType PgValue Int64
bigInt = mkSqlType "int8"

instance AutoType PgValue T.Text where
    auto = text
text :: SqlType PgValue T.Text
text = mkSqlType "text"

instance AutoType PgValue Bool where
    auto = bool
bool :: SqlType PgValue Bool
bool = mkSqlType "bool"

withBackend :: ConnectInfo -> (Backend PgValue -> IO a) -> IO a
withBackend connInfo =
    bracket (mkBackend <$> connect connInfo) connClose

mkBackend :: PS.Connection -> Backend PgValue
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

runQuery :: PS.Connection -> BS.ByteString -> [PgValue] -> IO [[PgValue]]
runQuery conn q vals = do
    print q
    unless (null vals) $
        print vals
    query conn (Query q) (fmap valToField vals)

runExecute :: PS.Connection -> BS.ByteString -> [PgValue] -> IO Int64
runExecute conn q vals = do
    print q
    unless (null vals) $
        print vals
    execute conn (Query q) (fmap valToField vals)

instance FromField PgValue where
    fromField = valFromField

valFromField :: FieldParser PgValue
valFromField f mv = do
    typ <- typename f
    case mv of
      Nothing ->
          pure SqlNull
      Just _ ->
          (case typ of
              "bool"        -> sqlField $ typeRep @Bool
              "int2"        -> sqlField $ typeRep @Int64
              "int4"        -> sqlField $ typeRep @Int64
              "int8"        -> sqlField $ typeRep @Int64
              "timestamp"   -> sqlField $ typeRep @UTCTime
              "timestamptz" -> sqlField $ typeRep @UTCTime
              "bpchar"      -> sqlField $ typeRep @T.Text
              "varchar"     -> sqlField $ typeRep @T.Text
              "text"        -> sqlField $ typeRep @T.Text
              _             -> sqlField $ typeRep @ByteString
          ) f mv


sqlField :: (Show a, ToField a, FromField a) => TypeRep a -> FieldParser PgValue
sqlField rep f mv = SqlField rep <$> fromField f mv

valToField :: PgValue -> Action
valToField (SqlField _ v) = toField v
valToField SqlNull        = Plain "NULL"
valToField SqlDefault     = Plain "DEFAULT"
