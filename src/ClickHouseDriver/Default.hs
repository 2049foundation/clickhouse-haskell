module ClickHouseDriver.Default
  ( username,
    hostname,
    password,
    portnumber,
    defaultHttpConnection,
  )
where

import ClickHouseDriver.Types
import Data.ByteString.Internal
import Network.HTTP.Client

{-default HTTP settings-}
username = "default"

hostname = "localhost"

password = "12345612341"

portnumber = 8123

defaultHttpConnection :: IO (ClickHouseConnection)
defaultHttpConnection = do
  mng <- newManager defaultManagerSettings
  return
    HttpConnection
      { httpHost = hostname,
        httpPassword = password,
        httpPort = portnumber,
        httpUsername = username,
        httpManager = mng
      }

defaultTCPConnection :: IO (ClickHouseConnection)
defaultTCPConnection = undefined