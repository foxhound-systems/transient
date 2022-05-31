module Transient.Internal.SqlDialect
    where

import qualified Data.ByteString as BS

data SqlDialect = SqlDialect
    { dialectEscapeIdentifier :: BS.ByteString -> BS.ByteString
    }
