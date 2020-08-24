{-# LANGUAGE OverloadedStrings #-}

module Test.QuerySpec (suite) where

import ClickHouseDriver.Core
import Data.ByteString as B
import Data.ByteString.Char8 as C8
import qualified Data.HashMap.Strict as HM
import Haxl.Core

suite :: IO ()
suite = do
  print "Test begin"