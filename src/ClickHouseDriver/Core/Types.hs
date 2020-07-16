{-# LANGUAGE MultiParamTypeClasses #-}

module ClickHouseDriver.Core.Types
  ( JSONResult (..),
    Cmd,
    Haxl,
  )
where

import Data.Aeson (Value)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as L
import Data.HashMap.Strict (HashMap)
import Data.Text (Text)
import Haxl.Core

type JSONResult = Either ByteString [HashMap Text Value] --Err LBS.ByteString | OK [HM.HashMap T.Text JP.Value]
      --deriving Show

type Cmd = String

type Haxl a = GenHaxl () a