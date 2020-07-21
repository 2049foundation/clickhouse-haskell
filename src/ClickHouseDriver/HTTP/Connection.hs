{-# LANGUAGE CPP  #-}
{-# LANGUAGE OverloadedStrings #-}


module ClickHouseDriver.HTTP.Connection (
    httpConnect,
    defaultHttpConnection,
    HttpConnection(..),
) where

import Data.ByteString.Builder
import ClickHouseDriver.IO.BufferedWriter
import ClickHouseDriver.IO.BufferedReader
import Network.Socket                                           
import qualified Network.Simple.TCP                        as TCP
import qualified Data.ByteString.Lazy                      as L     
import ClickHouseDriver.HTTP.Types
import Data.ByteString                                     hiding (unpack)
import Data.ByteString.Char8                               (unpack)
import Network.HTTP.Client
import Control.Monad.State.Lazy
import Data.Word
import qualified Data.ByteString.Char8 as C8
import Data.Int
import qualified Data.Binary as Binary

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  "12345612341"
--TODO change default password to ""

data HttpConnection
  = HttpConnection
      { httpHost ::                    !String,
        httpPort :: {-# UNPACK #-}     !Int,
        httpUsername ::                !String,
        httpPassword :: {-# UNPACK #-} !String,
        httpManager ::                 !Manager
      }

defaultHttpConnection :: IO (HttpConnection)
defaultHttpConnection = httpConnect DEFAULT_USERNAME DEFAULT_PASSWORD 8123 DEFAULT_HOST_NAME


httpConnect :: String->String->Int->String->IO(HttpConnection)
httpConnect user password port host = do
  mng <- newManager defaultManagerSettings
  return HttpConnection {
    httpHost = host,
    httpPassword = password,
    httpPort = port,
    httpUsername = user,
    httpManager = mng
  }
