module ClickHouseDriver.Core.Error (

) where

import Data.ByteString
import ClickHouseDriver.IO.BufferedReader

data ClickhouseException = ServerException

--TODO
readException :: Reader ClickhouseException
readException = undefined