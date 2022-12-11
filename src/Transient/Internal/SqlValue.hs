{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeFamilies     #-}

module Transient.Internal.SqlValue
    where

class (Eq (SqlValue be)) => SqlBackend be where
    data SqlValue be
    nullValue :: SqlValue be

