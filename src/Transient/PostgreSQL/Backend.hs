{-# LANGUAGE OverloadedStrings #-}
module Transient.PostgreSQL.Backend
    where

import           Control.Exception                    (bracket, throwIO)
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
import           Transient.Internal.SqlValue

withBackend :: ConnectInfo -> (Backend -> IO a) -> IO a
withBackend connInfo k =
    bracket
        (mkBackend <$> connect connInfo)
        (connClose)
        k


mkBackend :: PS.Connection -> Backend
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

runQuery :: PS.Connection -> BS.ByteString -> [SqlValue] -> IO [[SqlValue]]
runQuery conn q vals =
    unV $ query conn (Query q) (fmap valToField vals)

runExecute :: PS.Connection -> BS.ByteString -> [SqlValue] -> IO Int64
runExecute conn q vals =
    execute conn (Query q) (fmap valToField vals)

unV :: IO [[V]] -> IO [[SqlValue]]
unV = coerce

newtype V = V SqlValue
instance FromField V where
    fromField = valFromField

valFromField :: FieldParser V
valFromField f mv = do
    typ <- typename f
    case mv of
      Nothing ->
          pure $ V SqlNull
      Just v ->
        case typ of
          "bool"        -> V . SqlBool <$> fromField f mv
          "int2"        -> V . SqlInt <$> fromField f mv
          "int4"        -> V . SqlInt <$> fromField f mv
          "int8"        -> V . SqlInt <$> fromField f mv
          "timestamp"   -> V . SqlUTCTime <$> fromField f mv
          "timestamptz" -> V . SqlUTCTime <$> fromField f mv
          "bpchar"      -> V . SqlString <$> fromField f mv
          "varchar"     -> V . SqlString <$> fromField f mv
          "text"        -> V . SqlString <$> fromField f mv
          _             -> pure $ V $ SqlUnknown v

valToField :: SqlValue -> Action
valToField (SqlInt i)     = toField i
valToField (SqlString s)  = toField s
valToField (SqlUTCTime t) = toField t
valToField (SqlBool b)    = toField b
valToField SqlNull        = Plain "NULL"
valToField SqlDefault     = Plain "DEFAULT"
