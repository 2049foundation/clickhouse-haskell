{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module Main where

import           ClickHouseDriver.Core
import           ClickHouseDriver.Core.HTTP
import           Control.Monad.ST
import           Data.Text
import qualified Data.Text.IO as TIO
import           Network.HTTP.Client
import           Data.ByteString      hiding (putStr)  
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy.Char8 as CL8
import           Foreign.C
import           ClickHouseDriver.IO.BufferedWriter
import           ClickHouseDriver.IO.BufferedReader
import           Data.Monoid
import           Control.Monad.State.Lazy
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Word
import           Network.Socket                                           
import qualified Network.Simple.TCP as TCP
import qualified Data.Binary as B
import           Data.Int
import           Control.Monad.Writer
import qualified Data.Vector as V
import qualified Control.Monad.Reader as R
import           Control.Monad.Reader (ask)
import qualified ClickHouseDriver.Core.Column as Col
import           ClickHouseDriver.Core.HTTP.Helpers
import qualified Network.URI.Encode as NE
import qualified System.IO.Streams as Streams
import           System.IO hiding (putStr)
import           Data.Int
import           Data.Bits
import           Haxl.Core

main :: IO()
main = undefined