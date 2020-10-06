{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE NamedFieldPuns #-}

module ClickHouseDriver.Core.ConnectionPool 
(
  createConnectionPool,
  ConnParams(..)
) where

import           ClickHouseDriver.Core.Connection
import           ClickHouseDriver.Core.Defines
import           ClickHouseDriver.Core.Types      (Context, TCPConnection (..))
import qualified ClickHouseDriver.Core.Types      as Types
import           Data.ByteString
import           Data.Pool
import           Data.Time.Clock
import           Network.Socket

data ConnParams = ConnParams{
      username'    :: !ByteString,
      host'        :: !ByteString,
      port'        :: !ByteString,
      password'    :: !ByteString,
      compression' :: !Bool,
      database'    :: !ByteString
    }
  deriving Show

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