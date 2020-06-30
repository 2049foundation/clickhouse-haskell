module ClickHouseDriver.Default(
    username,
    dhost,
    password,
    dport,
    defaultConnection
) where 

import ClickHouseDriver.Types


{-default settings-}
username = "default"
dhost = "localhost"
password = ""
dport = 8123

defaultConnection :: ClickHouseConnection
defaultConnection = ClickHouseConnectionSettings {
     ciHost = dhost
    ,ciPassword = password
    ,ciPort = dport
    ,ciUsername = username
}