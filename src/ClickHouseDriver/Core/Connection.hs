module ClickHouseDriver.Core.Connection (

) where

import Data.ByteString.Builder
import ClickHouseDriver.IO.BufferedWriter
import Network.Socket
import qualified Network.Simple.TCP                        as T
import qualified Data.ByteString.Lazy                      as L
import ClickHouseDriver.Core.Helpers
import ClickHouseDriver.Core.Defines
import ClickHouseDriver.Core.ClientProtocol
import Data.ByteString





sendHello ::  (ByteString,ByteString,ByteString)->Socket->IO()
sendHello (database, usrname, password) sock  = do
    w <- writeVarUInt _HELLO mempty 
        >>= writeBinaryStr _CLIENT_NAME 
        >>= writeVarUInt _CLIENT_VERSION_MAJOR
        >>= writeVarUInt _CLIENT_VERSION_MINOR
        >>= writeVarUInt _CLIENT_REVISION
        >>= writeBinaryStr database
        >>= writeBinaryStr usrname
        >>= writeBinaryStr password
    T.sendLazy sock (toLazyByteString w)

--receiveHello :: 