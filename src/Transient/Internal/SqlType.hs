{-# LANGUAGE MultiParamTypeClasses #-}

module Transient.Internal.SqlType
    where

import qualified Data.ByteString             as BS
import           Transient.Internal.SqlValue

class AutoType be a where
    auto :: SqlType be a

data Context
    = QueryContext
    | CommandContext

data SqlType' be i o = SqlType
    { sqlType            :: BS.ByteString
    , sqlTypeConstraints :: [BS.ByteString]
    , toSqlType          :: Context -> i -> SqlValue be
    , fromSqlType        :: SqlValue be -> Maybe o
    }
type SqlType be a = SqlType' be a a
