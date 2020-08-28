{-# LANGUAGE OverloadedStrings #-}

module Test.QuerySpec (spec) where

import ClickHouseDriver.Core
import ClickHouseDriver.Core.HTTP
import Data.ByteString as B
import Data.ByteString.Char8 as C8
import qualified Data.HashMap.Strict as HM
import Haxl.Core
import Test.HUnit
import Test.Hspec


spec :: Spec
spec = parallel $ do
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
  res <- runIO $ execute "SELECT 1" conn
  it "returns result in ClickhouseType" $ do
    (show res) `shouldBe` (show [[CKUInt8 1]])

lowCardinalityTest :: Spec
lowCardinalityTest = describe "lowCardinality" $ do
  conn <- runIO $ defaultClient
  res <- runIO $ execute "SELECT * FROM crd3" conn
  it "returns result in ClickhouseType" $ do
    (show res) `shouldBe` (show [[CKFixedLengthString 3 "abc",CKString "myString",CKNull],[CKFixedLengthString 3 "xyz",CKString "Noctis",CKString "Ross"],[CKFixedLengthString 3 "123",CKString "Alice",CKNull],[CKFixedLengthString 3 "456",CKString "Bob",CKString "Walter"]])

comprehensiveTest :: Spec
comprehensiveTest = describe "array string number etc." $ do
  conn <- runIO $ defaultClient
  res <- runIO $ execute "SELECT * FROM big" conn
  it "returns result in ClickhouseType" $ do
    (show res) `shouldBe` (show "")