module ClickHouseDriver.Types (
    QueryResult (..),
    ClickHouseConnection (..),
    Url,
    Cmd,
    Haxl
) where

import qualified Data.Aeson                as JP
import qualified Data.ByteString.Lazy      as LBS
import qualified Data.HashMap.Strict       as HM
import qualified Data.Text                 as T
import           Haxl.Core

data QueryResult = Err LBS.ByteString | OK [HM.HashMap T.Text JP.Value]
                    deriving Show

data ClickHouseConnection = ClickHouseConnectionSettings {
     ciHost     :: {-# UNPACK #-}  !String
    ,ciPort     :: {-# UNPACK #-}  !Int
    ,ciUsername :: {-# UNPACK #-}  !String
    ,ciPassword :: {-# UNPACK #-}  !String
} deriving Show

type Url = String
type Cmd = String
type Haxl a = GenHaxl () a