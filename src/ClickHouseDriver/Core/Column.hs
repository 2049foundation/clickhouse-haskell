module ClickHouseDriver.TCP.Column where

import Data.Binary
import Data.ByteString
import Data.Vector



class Column a where
    