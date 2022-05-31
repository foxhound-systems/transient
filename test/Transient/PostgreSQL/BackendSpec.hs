{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
module Transient.PostgreSQL.BackendSpec
    where

import           Control.Monad               (join)
import           Data.Function               ((&))
import           Test.Hspec
import           Transient.Core
import           Transient.Internal.Backend
import           Transient.Internal.SqlValue
import           Transient.SqlQuery

import qualified Data.ByteString             as BS
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
    } deriving (Show, Eq)

testIdF :: EntityField Test (Maybe Int64) Int64
testIdF = "id" serial

testTestF :: SimpleField Test Int64
testTestF = "test"

testBoolF :: SimpleField Test (Maybe Bool)
testBoolF = "bool_field"

testT :: Table NewTest Test
testT = table "test" $ Test
                    <$> field newTestId testIdF
                    <*> field newTestTest testTestF
                    <*> field newTestBool testBoolF

spec :: SpecWith Backend
spec = do
    beforeWith (\conn -> createTable conn testT *> pure conn)
        do
            it "can insert new values with defaults" $ \conn -> do
                i <- insertMany conn testT [ NewTest Nothing 42 Nothing
                                           , NewTest Nothing 24 (Just True)
                                           ]
                i `shouldBe` 2
                x <- runSelectUnique conn $ do
                        t <- from testT
                        where_ $ t ^. testTestF ==. val 42
                        select t
                x `shouldBe` Just (Test 1 42 Nothing)
            it "can innerJoin" $ \conn -> do
                i <- insertMany conn testT [ NewTest (Just 1) 84 Nothing, NewTest (Just 2) 24 Nothing ]
                x <- runSelectMany conn $ do
                        t :& t2 <- from $ testT
                                & innerJoin_ testT do
                                    \(t :& t2) ->
                                        t ^. testIdF /=. t2 ^. testIdF
                        where_ $ t ^. testIdF ==. val 1
                        select (t ^. testTestF, t2 ^. testTestF)
                x `shouldBe` [(84, 24)]
