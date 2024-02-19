{-# LANGUAGE BlockArguments        #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE DuplicateRecordFields #-}

module Transient.PostgreSQL.BackendSpec
    where

import Control.Monad.Reader (ReaderT, runReaderT)
import Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Exception            (bracket)
import           Control.Monad                (join)
import           Data.Function                ((&))
import           Test.Hspec
import           Transient.Core
import           Transient.Internal.Backend
import           Transient.PostgreSQL.Backend (PgValue, serial)
import           Transient.SqlQuery

import qualified Data.ByteString              as BS
import           Data.Data                    (Data)
import           Data.Int                     (Int64)

data NewTest = NewTest
    { id   :: Maybe Int64
    , test :: Int64
    , bool :: Maybe Bool
    }
data Test = Test
    { id   :: Int64
    , test :: Int64
    , bool :: Maybe Bool
    } deriving (Show, Eq, Data)

data DTOTest = DTOTest
    { field1 :: Int64
    , field2 :: Int64
    } deriving (Show, Eq, Data)


testT :: Table PgValue NewTest Test
testT = table "test" $ Test
            <$> field (.id) ("id" serial)
            <*> field (.test) "test"
            <*> field (.bool) "bool"

inTransaction :: SpecWith (Backend PgValue) -> SpecWith (Backend PgValue)
inTransaction =
    aroundWith $ \action conn ->
        bracket
         (connExecute conn "BEGIN TRANSACTION" [])
         (\_ -> connExecute conn "ROLLBACK" [])
         (\_ -> action conn)

withTable :: SpecWith (Backend PgValue) -> SpecWith (Backend PgValue)
withTable =
    beforeAllWith $ \conn -> do
            connExecute conn "DROP TABLE IF EXISTS test" []
            createTable conn testT
            pure conn

asserting :: MonadIO m => Expectation -> m ()
asserting = liftIO

itDb :: String -> ReaderT (Backend sqlValue) IO () -> SpecWith (Backend sqlValue)
itDb desc = it desc . runReaderT

spec :: SpecWith (Backend PgValue)
spec = withTable $ inTransaction $ do
   itDb "can insert new values with defaults" do
       i <- insertMany testT [ NewTest Nothing 42 Nothing
                             , NewTest Nothing 24 (Just True)
                             ] 
       asserting $ i `shouldBe` 2
       x <- runSelectUnique do
               t <- from testT
               where_ $ t.test ==. val 42
               select t
       asserting $ x `shouldBe` Just (Test 1 42 Nothing)

   itDb "can do multiple joins, nested and regular" do
       i <- insertMany testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
       x <- runSelectMany do
                (t :& (t2 :& t3) :& t4) <-
                    from do
                      testT
                        & leftJoin_ do
                            testT
                              & innerJoin_ testT
                                `on_` do
                                    \(t :& t2) ->
                                        t.id ==. t2.id
                          `on_` do
                            \(t :& (_ :& t3)) ->
                                t.id /=. t3.id
                        & leftJoin_ testT
                          `on_` \(t :& _ :& t4) ->
                             t.id /=. t.id
                where_ $ t.id ==. val 1
                select (t.test :& t2.test :& t4.bool)
       asserting $ x `shouldBe` [84 :& Just 24 :& Nothing]

   itDb "can insert into" do
       i <- _insertInto testT do
              select do
                  Test <$> select_ (val 1)
                       <*> select_ (val 42)
                       <*> select_ (val Nothing)
       asserting $ i `shouldBe` 1
       x <- runSelectUnique do
               select =<< from testT
       asserting $ x `shouldBe` Just (Test 1 42 Nothing)

   itDb "can do subqueries" do 
       i <- insertMany testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
       x <- runSelectMany do
                (r :& r2) <- from $
                     from $
                     from (table_ testT & crossJoin_ testT)
                select (r :& r2.id)
       asserting $ x `shouldBe` [ (Test 1 84 Nothing :& 1), (Test 1 84 Nothing :& 2)
                                , (Test 2 24 Nothing :& 1), (Test 2 24 Nothing :& 2)
                                ]

   itDb "can use custom dtos" do
       i <- insertMany testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
       x <- runSelectMany do
                (r :& r2) <- from do
                       t <- from testT
                       let dtoTest = DTOTest <$> select_ t.id
                                             <*> select_ t.test
                       pure (dtoTest :& dtoTest)
                select (r :& r2.field1)
       asserting $ x `shouldBe` [ (DTOTest 1 84 :& 1), (DTOTest 2 24 :& 2) ]

