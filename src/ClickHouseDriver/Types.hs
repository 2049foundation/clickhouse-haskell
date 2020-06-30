module ClickHouseDriver.Types (
    QueryResult (..),
    ClickHouseConnection (..),
    Url,
    Cmd,
    Haxl
) where

import           Data.Aeson                (Value)
import           Data.ByteString.Lazy      (ByteString)
import           Data.HashMap.Strict       (HashMap)
import           Data.Text                 (Text)
import           Haxl.Core

type QueryResult = Either ByteString [HashMap Text Value] --Err LBS.ByteString | OK [HM.HashMap T.Text JP.Value]
                    --deriving Show

data ClickHouseConnection = ClickHouseConnectionSettings {
     ciHost     :: {-# UNPACK #-}  !String
    ,ciPort     :: {-# UNPACK #-}  !Int
    ,ciUsername :: {-# UNPACK #-}  !String
    ,ciPassword :: {-# UNPACK #-}  !String
} deriving Show

type Url = String
type Cmd = String
type Haxl a = GenHaxl () a