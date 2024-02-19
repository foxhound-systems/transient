module Transient.Internal.Backend
    where

import qualified Data.ByteString               as BS
import           Data.Int                      (Int64)
import           Transient.Internal.SqlDialect

data Backend value = Backend
    { connTag     :: BS.ByteString
    , connDialect :: SqlDialect
    , connQuery   :: BS.ByteString -> [value] -> IO [[value]]
    , connExecute :: BS.ByteString -> [value] -> IO Int64
    , connClose   :: IO ()
    }
