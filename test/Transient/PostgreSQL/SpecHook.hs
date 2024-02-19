{-# LANGUAGE OverloadedStrings #-}
module Transient.PostgreSQL.SpecHook
    where

import           Control.Exception            (bracket)
import           Database.PostgreSQL.Simple
import           Test.Hspec
import           Transient.Internal.Backend
import           Transient.PostgreSQL.Backend

connectInfo :: ConnectInfo
connectInfo = defaultConnectInfo{connectPassword="password"}

withConnection :: (Backend PgValue -> IO a) -> IO a
withConnection =
    withBackend connectInfo

withTransaction :: Backend PgValue -> (Backend PgValue -> IO a) -> IO a
withTransaction conn action =
    bracket
     (connExecute conn "BEGIN TRANSACTION" [])
     (\_ -> connExecute conn "ROLLBACK" [])
     (\_ -> action conn)

hook :: SpecWith (Backend PgValue) -> Spec
hook = aroundAll withConnection
