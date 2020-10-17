-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE NamedFieldPuns #-}
module ClickHouseDriver.Core.Pool 
(
  createConnectionPool
) where

import           ClickHouseDriver.Core.Connection
import           ClickHouseDriver.Core.Defines
import           ClickHouseDriver.Core.Types      (Context, TCPConnection (..))
import qualified ClickHouseDriver.Core.Types      as Types
import           Data.ByteString
import           Data.Pool
import           Data.Time.Clock
import           Network.Socket
import           Data.Default.Class
import           GHC.Generics 
import           ClickHouseDriver.Core.Types

instance Default ConnParams where
    def = ConnParams{
       username'    = _DEFAULT_USERNAME
      ,host'        = _DEFAULT_HOST_NAME
      ,port'        = _DEFAULT_PORT_NAME
      ,password'    = _DEFAULT_PASSWORD
      ,compression' = _DEFAULT_COMPRESSION_SETTING
      ,database'    = _DEFAULT_DATABASE
    }

createConnectionPool :: ConnParams
                      ->Int
                      ->NominalDiffTime
                      ->Int
                      ->IO (Pool TCPConnection)
createConnectionPool
  params@ConnParams
    { username',
      host',
      port',
      password',
      compression',
      database'
    }
  numStripes
  idleTime
  maxResources = createPool (do
      conn <- tcpConnect host' port' username' password' database' compression'
      case conn of
          Left err -> error err
          Right tcp -> return tcp
      ) (\TCPConnection{tcpSocket=sock}->close sock) 
      numStripes idleTime maxListenQueue

