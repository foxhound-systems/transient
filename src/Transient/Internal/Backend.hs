module Transient.Internal.Backend
    where

import qualified Data.ByteString               as BS
import           Data.Int                      (Int64)
import           Transient.Internal.SqlDialect
import           Transient.Internal.SqlValue

data Backend = Backend
    { connTag     :: BS.ByteString
    , connDialect :: SqlDialect
    , connQuery   :: BS.ByteString -> [SqlValue] -> IO [[SqlValue]]
    , connExecute :: BS.ByteString -> [SqlValue] -> IO Int64
    , connClose   :: IO ()
    }
