module Transient.Internal.Backend
    where

import qualified Data.ByteString               as BS
import           Data.Int                      (Int64)
import           Transient.Internal.SqlDialect
import           Transient.Internal.SqlValue

data Backend be = Backend
    { connTag     :: BS.ByteString
    , connDialect :: SqlDialect
    , connQuery   :: BS.ByteString -> [SqlValue be] -> IO [[SqlValue be]]
    , connExecute :: BS.ByteString -> [SqlValue be] -> IO Int64
    , connClose   :: IO ()
    }
