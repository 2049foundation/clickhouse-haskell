
{-# LANGUAGE MultiParamTypeClasses #-}
module ClickHouseDriver.Types (
    JSONResult (..),
    ClickHouseConnection (..),
    Cmd,
    Haxl,
) where

import           Data.Aeson           (Value)
import           Data.ByteString      (ByteString)
import qualified Data.ByteString.Lazy as L
import           Data.HashMap.Strict  (HashMap)
import           Data.Text            (Text)
import           Haxl.Core
import           Network.HTTP.Client  (Manager)


type JSONResult = Either ByteString [HashMap Text Value] --Err LBS.ByteString | OK [HM.HashMap T.Text JP.Value]
                    --deriving Show


data ClickHouseConnection = HttpConnection {
     httpHost     :: {-# UNPACK #-}  !String
    ,httpPort     :: {-# UNPACK #-}  !Int
    ,httpUsername :: {-# UNPACK #-}  !String
    ,httpPassword :: {-# UNPACK #-}  !String
    ,httpManager  :: {-# UNPACK #-}  !Manager
} |
 TCPConnection {
    tcpHost     :: {-# UNPACK #-} !String
   ,tcpPort     :: {-# UNPACK #-} !String
   ,tcpUsername :: {-# UNPACK #-}  !String
   ,tcpPassword :: {-# UNPACK #-}  !String
}



type Cmd = String
type Haxl a = GenHaxl () a
