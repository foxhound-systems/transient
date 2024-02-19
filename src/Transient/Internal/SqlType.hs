{-# LANGUAGE MultiParamTypeClasses #-}

module Transient.Internal.SqlType
    where

import qualified Data.ByteString as BS

class AutoType value a where
    auto :: SqlType value a

class SqlBackend value where
    isNullValue :: value -> Bool

data Context
    = QueryContext
    | CommandContext

data SqlType' value i o = SqlType
    { sqlType            :: BS.ByteString
    , sqlTypeIsNullable  :: Bool
    , sqlTypeConstraints :: [BS.ByteString]
    , toSqlType          :: Context -> i -> value
    , fromSqlType        :: value -> Maybe o
    }
type SqlType be a = SqlType' be a a
