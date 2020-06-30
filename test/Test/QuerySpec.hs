{-# LANGUAGE OverloadedStrings #-}

module Test.QuerySpec (spec) where

import           ClickHouseDriver
import           Data.ByteString.Lazy       as L
import           Data.ByteString.Lazy.Char8 as C8
import qualified Data.HashMap.Strict        as HM
import           Haxl.Core
import           Test.Hspec
import           Test.HUnit



{-
query1 = TestCase(do
    let deSetting = ClickHouseConnectionSettings {
                ciHost = "localhost",
                ciPassword = "12345612341",
                ciPort = 8123,
                ciUsername = "default"
            }
    env <- initEnv (stateSet (Settings deSetting) stateEmpty) ()
    res <- runHaxl env (getText "SHOW DATABASES")
    assertEqual "SHOW DATABASES" res (C8.pack "_temporary_and_external_tables\ndefault\nsystem\n"))


tests = TestList [TestLabel "query1" query1]
-}


deSetting = ClickHouseConnectionSettings {
            ciHost = "localhost",
            ciPassword = "12345612341",
            ciPort = 8123,
            ciUsername = "default"
        }

spec :: Spec
spec = parallel $ do
    query1
    query2
    query3

query1 :: Spec
query1 = describe "show databases" $ do
    env <- runIO $ defaultEnv deSetting
    res <- runIO $ runQuery env (getByteString "SHOW DATABASES")
    it "returns query result in text format" $ do
        res `shouldBe` C8.pack "DEMO\n_temporary_and_external_tables\ndefault\nsystem\n"

query2 :: Spec
query2 = describe "format in text" $ do
    env <- runIO $ defaultEnv deSetting
    res <- runIO $ runQuery env (getByteString "SELECT * FROM default.test_table")
    it "returns query result in text format" $ do
        res `shouldBe` C8.pack "0000000001\tJOHN\t1557\t[45,45,45]\n1234567890\tCONNOR\t533\t[1,2,3,4]\n3543364534\tMARRY\t220\t[0,1,2,3,121,2]\n2258864346\tJAME\t4452\t[42,-10988,66,676,0]\n"

query3 :: Spec
query3 = describe "format in JSON" $ do
    env <- runIO $ defaultEnv deSetting
    res <- runIO $ runQuery env (getJSON "SELECT * FROM default.test_table")
    let check = case res of
            Right (x:xs) -> C8.pack $ show (HM.lookup "id" x)
            _         -> C8.pack "error"
    it "returns query result in JSON" $ do
        check `shouldBe` C8.pack "Just (String \"0000000001\")"
