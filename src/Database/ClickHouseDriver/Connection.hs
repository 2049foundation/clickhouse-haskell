-- Copyright (c) 2020-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BlockArguments #-}

-- | This module contains the implementations of communication with Clickhouse server.
--   Most of functions are for internal use. 
--   User should just use Database.ClickHouseDriver.
--

module Database.ClickHouseDriver.Connection
  ( tcpConnect,
    sendQuery,
    receiveData,
    sendData,
    receiveResult,
    closeConnection,
    processInsertQuery,
    ping',
    versionTuple,
    sendCancel,
  )
where

import qualified Database.ClickHouseDriver.Block as Block
import qualified Database.ClickHouseDriver.ClientProtocol as Client
import Database.ClickHouseDriver.Column (transpose)
import Database.ClickHouseDriver.Defines
  ( _BUFFER_SIZE,
    _CLIENT_NAME,
    _CLIENT_REVISION,
    _CLIENT_VERSION_MAJOR,
    _CLIENT_VERSION_MINOR,
    _DBMS_MIN_REVISION_WITH_CLIENT_INFO,
    _DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
    _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
    _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
    _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES,
    _DBMS_MIN_REVISION_WITH_VERSION_PATCH,
    _DBMS_NAME,
    _DEFAULT_INSERT_BLOCK_SIZE,
    _STRINGS_ENCODING,
  )
import qualified Database.ClickHouseDriver.Error as Error
import qualified Database.ClickHouseDriver.QueryProcessingStage as Stage
import qualified Database.ClickHouseDriver.ServerProtocol as Server
import Database.ClickHouseDriver.Types
  ( Block (ColumnOrientedBlock, cdata),
    CKResult (CKResult),
    ClientInfo (ClientInfo),
    ClientSetting
      ( ClientSetting,
        insert_block_size,
        strings_as_bytes,
        strings_encoding
      ),
    Context (Context, client_info, client_setting, server_info),
    Interface (HTTP),
    Packet
      ( Block,
        EndOfStream,
        ErrorMessage,
        Hello,
        MultiString,
        Progress,
        StreamProfileInfo,
        queryData
      ),
    QueryInfo,
    ServerInfo (..),
    TCPConnection (..),
    getDefaultClientInfo,
    getServerInfo,
    readBlockStreamProfileInfo,
    readProgress,
    storeProfile,
    storeProgress,
    ClickhouseType(..)
  )
import Database.ClickHouseDriver.IO.BufferedReader
  ( Buffer (socket),
    Reader,
    createBuffer,
    readBinaryStr,
    readVarInt,
    refill,
  )
import Database.ClickHouseDriver.IO.BufferedWriter
  ( MonoidMap,
    Writer,
    writeBinaryStr,
    writeVarUInt,
  )
import Control.Monad.Loops (iterateWhile)
import Control.Monad.State.Lazy (get, runStateT)
import Control.Monad.Writer (WriterT (runWriterT), execWriterT)
import Control.Monad (when)
import Data.Maybe (fromMaybe)
import Data.ByteString (ByteString)
import Data.Foldable (forM_)
import Data.ByteString.Builder
  ( Builder,
    toLazyByteString,
  )
import Data.ByteString.Char8 (unpack)
import qualified Data.List as List (transpose)
import Data.List.Split (chunksOf)
import Data.Vector ((!))
import qualified Data.Vector as V
  ( concat,
    fromList,
    length,
    map,
  )
import qualified Network.Simple.TCP as TCP
  ( closeSock,
    connectSock,
    sendLazy,
  )
import Network.Socket (Socket)
import System.Timeout (timeout)

--Debug
--import Debug.Trace ( trace )

-- | This module mainly focuses how to make connection
-- | to clickhouse database and protocols to send and receive data
versionTuple :: ServerInfo -> (Word, Word, Word)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

-- | internal implementation for ping test. 
ping' :: Int
        -- ^ Time limit
       ->TCPConnection
        -- ^ host name, port number, and socket are needed
       ->IO (Maybe String)
        -- ^ response `ping`, or nothing indicating server is not properly connected.
ping' timeLimit TCPConnection {tcpHost = host, tcpPort = port, tcpSocket = sock} =
  timeout timeLimit $ do
    r <- execWriterT $ writeVarUInt Client._PING
    TCP.sendLazy sock (toLazyByteString r)
    buf <- createBuffer 1024 sock
    (packet_type, _) <- runStateT (iterateWhile (== Server._PROGRESS) readVarInt) buf
    if packet_type /= Server._PONG
      then do
        let p_type = Server.toString $ fromIntegral packet_type
        let report =
              "Unexpected packet from server " <> show host <> ":"
                <> show port
                <> ", expected "
                <> "Pong!"
                <> ", got "
                <> show p_type
        return $
          show
            Error.ServerException
              { code = Error._UNEXPECTED_PACKET_FROM_SERVER,
                message = report,
                nested = Nothing
              }
      else return "PONG!"

-- | send hello has to make for every new connection environment.
sendHello :: (ByteString, ByteString, ByteString)
              -- ^ (database name, username, password)
            ->Socket
              -- ^ socket connected to Clickhouse server 
            ->IO ()
sendHello (database, username, password) sock = do
  (_, w) <- runWriterT writeHello
  TCP.sendLazy sock (toLazyByteString w)
  where
    writeHello :: Writer Builder
    writeHello = do
      writeVarUInt Client._HELLO
      writeBinaryStr ("ClickHouse " <> _CLIENT_NAME)
      writeVarUInt _CLIENT_VERSION_MAJOR
      writeVarUInt _CLIENT_VERSION_MINOR
      writeVarUInt _CLIENT_REVISION
      writeBinaryStr database
      writeBinaryStr username
      writeBinaryStr password

-- | receive server information if connection is successful, otherwise it would receive error message.
receiveHello :: Buffer
              -- ^ Read `Hello` response from server
              ->IO (Either ByteString ServerInfo)
              -- ^ Either error message or server information will be received.
receiveHello buf = do
  (res, _) <- runStateT receiveHello' buf
  return res
  where
    receiveHello' :: Reader (Either ByteString ServerInfo)
    receiveHello' = do
      packet_type <- readVarInt
      if packet_type == Server._HELLO
        then do
          server_name <- readBinaryStr
          server_version_major <- readVarInt
          server_version_minor <- readVarInt
          server_revision <- readVarInt
          server_timezone <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
              then do Just <$> readBinaryStr
              else return Nothing
          server_display_name <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
              then do
                readBinaryStr
              else return ""
          server_version_dispatch <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
              then do
                readVarInt
              else return server_revision
          return $
            Right
              ServerInfo
                { name = server_name,
                  version_major = server_version_major,
                  version_minor = server_version_minor,
                  version_patch = server_version_dispatch,
                  revision = server_revision,
                  timezone = server_timezone,
                  display_name = server_display_name
                }
        else
          if packet_type == Server._EXCEPTION
            then do
              e <- readBinaryStr
              e2 <- readBinaryStr
              e3 <- readBinaryStr
              return $ Left ("exception" <> e <> " " <> e2 <> " " <> e3)
            else return $ Left "Error"

-- | connect to database through TCP port, used in Client module.
tcpConnect ::
  -- | host name to connect
  ByteString ->
  -- | port name to connect. Default would be 8123
  ByteString ->
  -- | username. Default would be "default"
  ByteString ->
  -- | password. Default would be empty string
  ByteString ->
  -- | database. Default would be "default"
  ByteString ->
  -- | choose if send and receive data in compressed form. Default would be False.
  Bool ->
  IO (Either String TCPConnection)
tcpConnect host port user password database compression = do
  (sock, sockaddr) <- TCP.connectSock (unpack host) (unpack port)
  sendHello (database, user, password) sock
  let isCompressed = if compression then Client._COMPRESSION_ENABLE else Client._COMPRESSION_DISABLE

  buf <- createBuffer _BUFFER_SIZE sock
  hello <- receiveHello buf
  case hello of
    Right x ->
      return $
        Right
          TCPConnection
            { tcpHost = host,
              tcpPort = port,
              tcpUsername = user,
              tcpPassword = password,
              tcpSocket = sock,
              tcpSockAdrr = sockaddr,
              context =
                Context
                  { server_info = Just x,
                    client_info = Nothing,
                    client_setting =
                      Just $
                        ClientSetting
                          { insert_block_size = _DEFAULT_INSERT_BLOCK_SIZE,
                            strings_as_bytes = False,
                            strings_encoding = _STRINGS_ENCODING
                          }
                  },
              tcpCompression = isCompressed
            }
    Left "Exception" -> return $ Left "exception"
    Left x -> do
      print ("error is " <> x)
      TCP.closeSock sock
      return $ Left "Connection error"

sendQuery :: TCPConnection
           -- ^ To get socket and server info
           ->ByteString
           -- ^ SQL statement
           ->Maybe ByteString
           -- ^ query_id if any
           ->IO ()
sendQuery TCPConnection {context = Context {server_info = Nothing}} _ _ = error "Empty server info"
sendQuery
  TCPConnection
    { tcpCompression = comp,
      tcpSocket = sock,
      context =
        Context
          { server_info = Just info
          }
    }
  query
  query_id = do
    (_, r) <- runWriterT $ do

      writeVarUInt Client._QUERY
      writeBinaryStr $ fromMaybe "" query_id
      let revision' = revision info
      when (revision' >= _DBMS_MIN_REVISION_WITH_CLIENT_INFO)
          $ do 
         let client_info
              = getDefaultClientInfo (_DBMS_NAME <> " " <> _CLIENT_NAME)
         writeInfo client_info info
      writeVarUInt 0 -- TODO add write settings
      writeVarUInt Stage._COMPLETE
      writeVarUInt comp
      writeBinaryStr query
    TCP.sendLazy sock (toLazyByteString r)

sendData :: TCPConnection
            -- ^ To get socket and context
          ->ByteString
            -- ^ table name 
          ->Maybe Block
            -- ^ a block data if any
          ->IO ()
sendData TCPConnection {tcpSocket = sock, context = ctx} table_name maybe_block = do
  -- TODO: ADD REVISION
  let info = Block.defaultBlockInfo
  r <- execWriterT $ do
    writeVarUInt Client._DATA
    writeBinaryStr table_name
    case maybe_block of
      Nothing -> do
        Block.writeInfo info
        writeVarUInt 0 -- #col
        writeVarUInt 0 -- #row
      Just block -> do
        Block.writeBlockOutputStream ctx block
  TCP.sendLazy sock $ toLazyByteString r

-- | Cancel last query sent to server
sendCancel :: TCPConnection -> IO ()
sendCancel TCPConnection {tcpSocket = sock} = do
  c <- execWriterT $ writeVarUInt Client._CANCEL
  TCP.sendLazy sock (toLazyByteString c)

processInsertQuery ::
  -- | source
  TCPConnection ->
  -- | query without data
  ByteString ->
  -- | query id
  Maybe ByteString ->
  -- | data in Haskell type.
  [[ClickhouseType]] ->
  -- | return 1 if insertion successfully completed
  IO ByteString
processInsertQuery
  tcp@TCPConnection {tcpSocket = sock, context = Context {client_setting = client_setting}}
  query_without_data
  query_id
  items = do
    sendQuery tcp query_without_data query_id
    sendData tcp "" Nothing
    buf <- createBuffer 2048 sock
    let info = case getServerInfo tcp of
          Nothing -> error "empty server info"
          Just s -> s
    (sample_block, _) <-
      runStateT
        ( iterateWhile
            ( \case
                Block {..} -> False
                MultiString _ -> True
                Hello -> True
                x -> error ("unexpected packet type: " ++ show x)
            )
            $ receivePacket info
        )
        buf
    case client_setting of
      Nothing -> error "empty client settings"
      Just ClientSetting {insert_block_size = siz} -> do
        let chunks = chunksOf (fromIntegral siz) items
        mapM_
          ( \chunk -> do
              let vectorized = V.map V.fromList (V.fromList $ List.transpose chunk)
              let dataBlock = case sample_block of
                    Block
                      typeinfo@ColumnOrientedBlock
                        { columns_with_type = cwt
                        } ->
                        typeinfo
                          { cdata = vectorized
                          }
                    x -> error ("unexpected packet type: " ++ show x)
              sendData tcp "" (Just dataBlock)
          )
          chunks
        sendData tcp "" Nothing
        buf2 <- refill buf
        runStateT (receivePacket info) buf2
        return "1"

-- | Read data from stream.
receiveData :: ServerInfo -> Reader Block.Block
receiveData info@ServerInfo {revision = revision} = do
  _ <-
    if revision >= _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
      then readBinaryStr
      else return ""
  Block.readBlockInputStream info


-- | Transform received query data into Clickhouse type
receiveResult :: ServerInfo
                -- ^ Server information
              -> QueryInfo
               -- ^ Query information
              -> Reader (Either String CKResult)
              -- ^ Receive either error message or query result.
receiveResult info query_info = do
  packets <- packetGen
  let onlyDataPacket = filter isBlock packets
  let errors = (\(ErrorMessage str) -> str) <$> filter isError packets
  case errors of
    [] -> do
      let dataVectors = Block.cdata . queryData <$> onlyDataPacket
      let newQueryInfo = Prelude.foldl updateQueryInfo query_info packets
      return $ Right $ CKResult (V.concat dataVectors) newQueryInfo
    xs -> do
      return $ Left $ Prelude.concat xs
  where
    updateQueryInfo :: QueryInfo -> Packet -> QueryInfo
    updateQueryInfo q (Progress prog) =
      storeProgress q prog
    updateQueryInfo q (StreamProfileInfo profile) =
      storeProfile q profile
    updateQueryInfo q _ = q

    isError :: Packet -> Bool
    isError (ErrorMessage _) = True
    isError _ = False

    isBlock :: Packet -> Bool
    isBlock Block {queryData = Block.ColumnOrientedBlock {cdata = d}} =
      V.length d > 0 && V.length (d ! 0) > 0
    isBlock _ = False

    packetGen :: Reader [Packet]
    packetGen = do
      packet <- receivePacket info
      case packet of
        EndOfStream -> return []
        error@(ErrorMessage _) -> return [error]
        _ -> do
          next <- packetGen
          return (packet : next)

-- | Receive data packet from server 
receivePacket :: ServerInfo -> Reader Packet
receivePacket info = do
  packet_type <- readVarInt
  -- The pattern matching does not support match with variable name,
  -- so here we use number instead.
  case packet_type of
    1 -> receiveData info >>= (return . Block) -- Data
    2 -> Error.readException Nothing >>= (return . ErrorMessage . show) -- Exception
    3 -> readProgress (revision info) >>= (return . Progress) -- Progress
    5 -> return EndOfStream -- End of Stream
    6 -> readBlockStreamProfileInfo >>= (return . StreamProfileInfo) --Profile
    7 -> receiveData info >>= (return . Block) -- Total
    8 -> receiveData info >>= (return . Block) -- Extreme
    -- 10 -> return undefined -- Log
    11 -> do
      -- MultiStrings message
      first <- readBinaryStr
      second <- readBinaryStr
      return $ MultiString (first, second)
    0 -> return Hello -- Hello
    _ -> do
      closeBufferSocket
      error $
        show
          Error.ServerException
            { code = Error._UNKNOWN_PACKET_FROM_SERVER,
              message = "Unknown packet from server",
              nested = Nothing
            }

closeConnection :: TCPConnection -> IO ()
closeConnection TCPConnection {tcpSocket = sock} = TCP.closeSock sock

-- | write client information and server infomation to protocols
writeInfo ::
  (MonoidMap ByteString w) =>
  ClientInfo ->
  ServerInfo ->
  Writer w
writeInfo
  ( ClientInfo
      client_name
      interface
      client_version_major
      client_version_minor
      client_version_patch
      client_revision
      initial_user
      initial_query_id
      initial_address
      quota_key
      query_kind
    )
  ServerInfo {revision = server_revision, display_name = host_name}
    | server_revision < _DBMS_MIN_REVISION_WITH_CLIENT_INFO =
      error "Method writeInfo is called for unsupported server revision"
    | otherwise = do
      writeVarUInt 1
      writeBinaryStr initial_user
      writeBinaryStr initial_query_id
      writeBinaryStr initial_address
      writeVarUInt (if interface == HTTP then 0 else 1)
      writeBinaryStr "" -- os_user. Seems that haskell modules don't have support of getting system username yet.
      writeBinaryStr host_name
      writeBinaryStr _CLIENT_NAME --client_name
      writeVarUInt client_version_major
      writeVarUInt client_version_minor
      writeVarUInt client_revision
      when
        (server_revision >= _DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
        $ writeBinaryStr quota_key
      when 
        (server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH)
        $ writeVarUInt client_version_patch

-------------------------------------------------------------------------------------------------------------------
---Helpers
{-# INLINE closeBufferSocket #-}
closeBufferSocket :: Reader ()
closeBufferSocket = do
  buf <- get
  let sock = Database.ClickHouseDriver.IO.BufferedReader.socket buf
  forM_ sock TCP.closeSock