module ClickHouseDriver.Default(
    username,
    dhost,
    password,
    dport,
    defaultConnection
) where 

import ClickHouseDriver.Types
import Network.HTTP.Client

{-default settings-}
username = "default"
dhost = "localhost"
password = ""
dport = 8123



defaultConnection :: IO (ClickHouseConnection)
defaultConnection = do
    mng <- newManager defaultManagerSettings
    return ClickHouseConnectionSettings {
     ciHost = dhost
    ,ciPassword = password
    ,ciPort = dport
    ,ciUsername = username
    ,ciManager  = mng
  }