module ClickHouseDriver.Default(
    usr,
    hst,
    pw,
    prt,
    defaultHttpConnection
) where

import           ClickHouseDriver.Types
import           Data.ByteString.Internal
import           Network.HTTP.Client

{-default HTTP settings-}
usr = "default"
hst = "localhost"
pw = ""
prt = 8123


defaultHttpConnection :: IO (ClickHouseConnection)
defaultHttpConnection = do
    mng <- newManager defaultManagerSettings
    return HttpConnection {
     httpHost = hst
    ,httpPassword = pw
    ,httpPort = prt
    ,httpUsername = usr
    ,httpManager  = mng
  }

defaultTCPConnection :: IO (ClickHouseConnection)
defaultTCPConnection = undefined
