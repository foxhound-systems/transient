{-# LANGUAGE BlockArguments        #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels      #-}
{-# LANGUAGE OverloadedStrings     #-}

module Transient.PostgreSQL.BackendSpec
    where

import           Control.Exception           (bracket)
import           Control.Monad               (join)
import           Data.Function               ((&))
import           Test.Hspec
import           Transient.Core
import           Transient.Internal.Backend
import           Transient.Internal.SqlValue
import           Transient.SqlQuery

import qualified Data.ByteString             as BS
import           Data.Data                   (Data)
import           Data.Int                    (Int64)

data NewTest = NewTest
    { newTestId   :: Maybe Int64
    , newTestTest :: Int64
    , newTestBool :: Maybe Bool
    }
data Test = Test
    { testId   :: Int64
    , testTest :: Int64
    , testBool :: Maybe Bool
    } deriving (Show, Eq, Data)

data DTOTest = DTOTest
    { dtoField1 :: Int64
    , dtoField2 :: Int64
    } deriving (Show, Eq, Data)

testT :: Table NewTest Test
testT = table "test" $ Test
            <$> field newTestId ("id" serial)
            <*> field newTestTest "test"
            <*> field newTestBool "bool"

inTransaction :: SpecWith Backend -> SpecWith Backend
inTransaction =
    aroundWith $ \action conn ->
        bracket
         (connExecute conn "BEGIN TRANSACTION" [])
         (\_ -> connExecute conn "ROLLBACK" [])
         (\_ -> action conn)

withTable :: SpecWith Backend -> SpecWith Backend
withTable =
    beforeAllWith $
        \conn -> do
            connExecute conn "DROP TABLE IF EXISTS test" []
            createTable conn testT
            pure conn

spec :: SpecWith Backend
spec = withTable $ inTransaction $ do
   it "can insert new values with defaults" $ \conn -> do
       i <- insertMany conn testT [ NewTest Nothing 42 Nothing
                                  , NewTest Nothing 24 (Just True)
                                  ]
       i `shouldBe` 2
       x <- runSelectUnique conn do
               t <- from testT
               where_ $ t ^. #testTest ==. val 42
               select t
       x `shouldBe` Just (Test 1 42 Nothing)

   it "can do multiple joins, nested and regular" $ \conn -> do
       i <- insertMany conn testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
       x <- runSelectMany conn do
                (t :& (t2 :& t3) :& t4) <-
                    from do
                      testT
                        & leftJoin_ do
                            testT
                              & innerJoin_ testT
                                `on_` do
                                    \(t :& t2) ->
                                        t ^. #testId ==. t2 ^. #testId
                          `on_` do
                            \(t :& (_ :& t3)) ->
                                t ^. #testId /=. t3 ^. #testId
                        & leftJoin_ testT
                          `on_` \(t :& _ :& t4) ->
                             t ^. #testId /=. t ^. #testId
                where_ $ t ^. #testId ==. val 1
                select (t ^. #testTest :& t2 ?. #testTest)
       x `shouldBe` [84 :& Just 24]

   it "can insert into" $ \conn -> do
       i <- insertInto conn testT do
              select do
                  Test <$> select_ (val 1)
                       <*> select_ (val 42)
                       <*> select_ (val Nothing)
       i `shouldBe` 1
       x <- runSelectUnique conn do
               select =<< from testT
       x `shouldBe` Just (Test 1 42 Nothing)

   it "can do subqueries" $ \conn -> do
       i <- insertMany conn testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
       x <- runSelectMany conn do
                (r :& r2) <- from $
                     from $
                     from (table_ testT & crossJoin_ testT)
                select (r :& r2 ^. #testId)
       x `shouldBe` [ (Test 1 84 Nothing :& 1), (Test 1 84 Nothing :& 2)
                    , (Test 2 24 Nothing :& 1), (Test 2 24 Nothing :& 2)
                    ]

   it "can use custom dtos" $ \conn -> do
       i <- insertMany conn testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
       x <- runSelectMany conn do
                r <- from do
                       t <- from testT
                       select $
                        DTOTest
                            <$> select_ (t ^. #testId)
                            <*> select_ (t ^. #testTest)
                select (r :& r ^. #dtoField1)
       x `shouldBe` [ (DTOTest 1 84 :& 1), (DTOTest 2 24 :& 2) ]

