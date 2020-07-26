module ClickHouseDriver.Core.Context (

) where

import ClickHouseDriver.Core.Types

newtype Context = Context {
    server_info :: ServerInfo
}