{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE CPP  #-}

module ClickHouseDriver.Core.Client (

) where

import ClickHouseDriver.Core.Connection
import ClickHouseDriver.Core.Block
import ClickHouseDriver.Core.Defines
import Haxl.Core

import qualified Data.ByteString as BS

data Query a where
    GetData :: String->Query String
