module Transient.Internal.SqlType
    where

import qualified Data.ByteString             as BS
import           Transient.Internal.SqlValue

data SqlType' i o = SqlType
    { sqlType            :: BS.ByteString
    , sqlTypeConstraints :: [BS.ByteString]
    , toSqlType          :: i -> SqlValue
    , fromSqlType        :: SqlValue -> Maybe o
    }
type SqlType a = SqlType' a a
