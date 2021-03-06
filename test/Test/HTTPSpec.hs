{-# LANGUAGE OverloadedStrings #-}

module Test.HTTPSpec (httpSpec) where

import Database.ClickHouseDriver
import Database.ClickHouseDriver.HTTP
import Data.ByteString as B
import Data.ByteString.Char8 as C8
import qualified Data.HashMap.Strict as HM
import Haxl.Core
import Test.HUnit
import Test.Hspec
import Data.Either


httpSpec :: IO()
httpSpec = hspec $ parallel $ do
  query1
  query2
  query3
  queryTCP
  lowCardinalityTest

query1 :: Spec
query1 = describe "show databases" $ do
  deSetting <- runIO $ defaultHttpConnection
  env <- runIO $ setupEnv deSetting
  res <- runIO $ runQuery env (getByteString "SHOW DATABASES")
  it "returns query result in text format" $ do
    res `shouldBe` C8.pack "_temporary_and_external_tables\ndefault\nsystem\n"

query2 :: Spec
query2 = describe "select 1" $ do
  deSetting <- runIO $ defaultHttpConnection
  env <- runIO $ setupEnv deSetting
  res <- runIO $ runQuery env (getByteString "SELECT 1")
  it "returns query result in text format" $ do
    res `shouldBe` C8.pack "1\n"

query3 :: Spec
query3 = describe "format in JSON" $ do
  deSetting <- runIO $ defaultHttpConnection
  env <- runIO $ setupEnv deSetting
  res <- runIO $ runQuery env (getJSON "SELECT * FROM default.test_table")
  let check = case res of
        Right (x : xs) -> C8.pack $ show (HM.lookup "id" x)
        _ -> C8.pack "error"
  it "returns query result in JSON" $ do
    check `shouldBe` C8.pack "Just (String \"0000000011\")"

queryTCP :: Spec
queryTCP = describe "select 1" $ do
  conn <- runIO $ defaultClient
  res <- runIO $ query conn "SELECT 1" 
  it "returns result in ClickhouseType" $ do
    (show res) `shouldBe` ("Right [[CKUInt8 1]]")

lowCardinalityTest :: Spec
lowCardinalityTest = describe "lowCardinality" $ do
  conn <- runIO $ defaultClient
  res <- runIO $ query conn "SELECT * FROM crd3" 
  it "returns result in ClickhouseType" $ do
    (show res) `shouldBe` ("Right " ++ show [[CKString "abc",CKString "myString",CKNull],[CKString"xyz",CKString "Noctis",CKString "Ross"],[CKString "123",CKString "Alice",CKNull],[CKString "456",CKString "Bob",CKString "Walter"]])

comprehensiveTest :: Spec
comprehensiveTest = describe "array string number etc." $ do
  conn <- runIO $ defaultClient
  res <- runIO $ query conn "SELECT * FROM big" 
  it "returns result in ClickhouseType" $ do
    (show res) `shouldBe` (show "")