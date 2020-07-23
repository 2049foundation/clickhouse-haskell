{-# LANGUAGE CPP  #-}
{-# LANGUAGE OverloadedStrings #-}


module ClickHouseDriver.Core.HTTP.Connection (
    httpConnect,
    defaultHttpConnection,
    HttpConnection(..),
) where
                                
import Network.HTTP.Client

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
