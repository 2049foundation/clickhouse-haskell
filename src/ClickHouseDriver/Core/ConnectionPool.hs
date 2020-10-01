{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE NamedFieldPuns #-}

module ClickHouseDriver.Core.ConnectionPool () where

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

data CKPool = CKPool {
    params :: !ConnParams,
    pool   :: IO (Pool (Socket, SockAddr, Context, Word))
}

createConnectionPool :: ConnParams -> Int -> NominalDiffTime -> Int -> IO CKPool
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
  maxResources =
    return
      CKPool
        { params = params,
          pool =
            createPool
              ( do
                  conn <- tcpConnect host' port' username' password' database' compression'
                  case conn of
                    Right
                      TCPConnection
                        { Types.context = ctx,
                          Types.tcpSocket = sock,
                          Types.tcpSockAdrr = sockaddr,
                          Types.tcpCompression = comp
                        } -> return (sock, sockaddr, ctx, comp)
                    Left err -> error "connection failed!"
              )
              (\(sock,_,_,_)->close sock)
              numStripes
              idleTime
              maxResources
        }
