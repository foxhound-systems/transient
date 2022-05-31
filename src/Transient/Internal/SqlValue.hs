module Transient.Internal.SqlValue
    where

import qualified Data.ByteString as BS
import           Data.Int        (Int64)
import qualified Data.Text       as T
import           Data.Time.Clock (UTCTime)

data SqlValue
    = SqlString T.Text
    | SqlInt Int64
    | SqlUTCTime UTCTime
    | SqlBool Bool
    | SqlNull
    | SqlDefault
    | SqlUnknown BS.ByteString
    deriving (Eq, Show)

