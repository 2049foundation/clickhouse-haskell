-- | Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
module ClickHouseDriver.Core.HTTP.Types
  ( JSONResult (..),
    Cmd,
    Haxl,
    Format(..)
  )
where

import           Data.Aeson           (Value)
import           Data.ByteString      (ByteString)
import qualified Data.ByteString.Lazy as L
import           Data.HashMap.Strict  (HashMap)
import           Data.Text            (Text)
import           Haxl.Core

type JSONResult = Either ByteString [HashMap Text Value]

type Cmd = String

type Haxl a = GenHaxl () a

data Format = CSV | JSON | TUPLE
    deriving Eq
