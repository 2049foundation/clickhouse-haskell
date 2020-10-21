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

import ClickHouseDriver.Core.Connection ( tcpConnect )
import ClickHouseDriver.Core.Defines
    ( _DEFAULT_USERNAME,
      _DEFAULT_HOST_NAME,
      _DEFAULT_PASSWORD,
      _DEFAULT_PORT_NAME,
      _DEFAULT_DATABASE,
      _DEFAULT_COMPRESSION_SETTING )
import Data.Pool ( createPool, Pool )
import Data.Time.Clock ( NominalDiffTime )
import Network.Socket (close)
import Data.Default.Class ( Default(..) )
import ClickHouseDriver.Core.Types
    ( ConnParams(..), TCPConnection(TCPConnection, tcpSocket) )

-- | default connection parameters (settings)
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
  ConnParams
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
      numStripes idleTime maxResources

