{-# LANGUAGE BlockArguments #-}
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

-- | This module contains the implementations of communication with Clickhouse server.
--   Most of functions are for internal use.
--   User should just use Database.ClickHouseDriver.
module Database.ClickHouseDriver.Connection
  (
    withConnect,
    sendQuery,
    sendData,
    receiveData,
    receiveResult,
    processInsertQuery,
    ping',
    versionTuple,
  )
where

import Control.Monad (when)
import Control.Monad.Loops (iterateWhile)
import Data.Foldable (forM_)
import qualified Data.List as List (transpose)
import Data.List.Split (chunksOf)
import Data.Maybe (fromMaybe, isNothing)
import qualified Database.ClickHouseDriver.Block as Block
import qualified Database.ClickHouseDriver.ClientProtocol as Client
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
import Database.ClickHouseDriver.IO.BufferedReader
  ( Buffer (socket),
    readBinaryStr,
    readVarInt,
  )
import Database.ClickHouseDriver.IO.BufferedWriter
  ( writeBinaryStr,
    writeVarUInt,
  )
import qualified Database.ClickHouseDriver.QueryProcessingStage as Stage
import qualified Database.ClickHouseDriver.ServerProtocol as Server
import Database.ClickHouseDriver.Types
  ( Block (ColumnOrientedBlock, cdata),
    CKResult (CKResult),
    ClickhouseType (..),
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
    readBlockStreamProfileInfo,
    readProgress,
    storeProfile,
    storeProgress, ConnParams (..)
  )
import System.Timeout (timeout)
import qualified Z.Data.Builder as B
import Z.Data.CBytes (buildCBytes)
import qualified Z.Data.Parser as P
import Z.Data.Vector (Bytes)
import qualified Z.Data.Vector as Z
import qualified Z.IO.Buffered as ZB
import qualified Z.IO.Network as ZIO
import Z.IO.Resource (withResource)
import Z.IO.UV.UVStream (UVStream)
import Z.Data.ASCII ( w2c )
import Data.Functor ( (<&>) )
import qualified ListT as L

--Debug
import Debug.Trace ( trace )

printBytes :: Bytes->String
printBytes b = w2c <$> Z.unpack b

-- | This module mainly focuses how to make connection
-- | to clickhouse database and protocols to send and receive data
versionTuple :: ServerInfo -> (Word, Word, Word)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

-- | resolve DNS
resolveDNS :: (ZIO.HostName, ZIO.PortNumber) -> IO ZIO.AddrInfo
resolveDNS (h, p) = head <$> ZIO.getAddrInfo Nothing h (buildCBytes . B.int $ p)

-- | internal implementation for ping test.
ping' ::
  ConnParams ->
  -- | Time limit
  Int ->
  -- | host name, port number, and socket are needed
  (ZB.BufferedInput, ZB.BufferedOutput) ->
  -- | response `ping`, or nothing indicating server is not properly connected.
  IO (Maybe String)
ping' ConnParams{host' = host, port' = port} timeLimit (i, o) =
  timeout timeLimit $ do
    let r = B.build $ writeVarUInt Client._PING
    ZB.writeBuffer' o r
    packet_type <- ZB.readParser (iterateWhile (== Server._PROGRESS) readVarInt) i
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
sendHello ::
  -- | (database name, username, password)
  (Bytes, Bytes, Bytes) ->
  -- | socket connected to Clickhouse server
  ZB.BufferedOutput ->
  IO ()
sendHello (database, username, password) o = do
  let w = B.build writeHello
  ZB.writeBuffer' o w
  where
    writeHello :: B.Builder ()
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
receiveHello ::
  -- | Read `Hello` response from server
  ZB.BufferedInput ->
  -- | Either error message or server information will be received.
  IO (Either Bytes ServerInfo)
receiveHello = ZB.readParser receiveHello'
  where
    receiveHello' :: P.Parser (Either Bytes ServerInfo)
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
              then Just <$> readBinaryStr
              else return Nothing
          server_display_name <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
              then
                readBinaryStr
              else return ""
          server_version_dispatch <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
              then
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

withConnect :: ConnParams -> ( (ZB.BufferedInput, ZB.BufferedOutput, Context) -> IO ()) -> IO ()
withConnect (ConnParams host port username password database comp) f = do
  addr <- resolveDNS (host, port)
  let tcpconf =
        ZIO.defaultTCPClientConfig
          { ZIO.tcpRemoteAddr = ZIO.addrAddress addr
          }
  let resource = ZIO.initTCPClient tcpconf
  withResource resource $ \tcp -> do
    (i, o) <- ZB.newBufferedIO tcp
    sendHello (database, username, password) o
    let isCompressed =
          if comp
            then Client._COMPRESSION_ENABLE
            else Client._COMPRESSION_DISABLE
    hello <- receiveHello i
    case hello of
      Left "Exception" -> error "exception"
      Left x -> do
        print ("error is " <> x)
        error "Connection error"
      Right x -> do
        let client_setting =
              Just $ ClientSetting
                { insert_block_size = _DEFAULT_INSERT_BLOCK_SIZE,
                  strings_as_bytes = False,
                  strings_encoding = _STRINGS_ENCODING
                }
        let client_info = Nothing
            server_info = x
            ctx = Context client_info server_info client_setting
        f (i, o, ctx)
sendQuery ::
  ZB.BufferedOutput ->
    -- | To get socket and server info
  ServerInfo ->
  -- | SQL statement
  Bytes ->
  -- | query_id if any
  Maybe Bytes ->
  IO ()
sendQuery
  output
  info
  query
  query_id = do
      let revision' = revision info
          r = B.build $ do
            writeVarUInt Client._QUERY
            writeBinaryStr $ fromMaybe "" query_id
            when (revision' >= _DBMS_MIN_REVISION_WITH_CLIENT_INFO) $ do
              let client_info = getDefaultClientInfo (_DBMS_NAME <> " " <> _CLIENT_NAME)
              writeInfo client_info info
            writeVarUInt 0 -- TODO add write settings
            writeVarUInt Stage._COMPLETE
            writeVarUInt 0
            writeBinaryStr query
      ZB.writeBuffer' output r

sendData ::
  ZB.BufferedOutput ->
  Context ->
  -- | table name
  Bytes ->
  -- | a block data if any
  Maybe Block ->
  IO ()
sendData output ctx table_name maybe_block = do
  -- TODO: ADD REVISION
  let info = Block.defaultBlockInfo
      r = B.build $ do
        writeVarUInt Client._DATA
        writeBinaryStr table_name
        case maybe_block of
          Nothing -> do
            Block.writeInfo info
            writeVarUInt 0 -- #col
            writeVarUInt 0 -- #row
          Just block ->
            Block.writeBlockOutputStream ctx block
  ZB.writeBuffer' output r

sendCancel :: ZB.BufferedOutput -> IO ()
sendCancel out = do
  let c = B.build $ writeVarUInt Client._CANCEL
  ZB.writeBuffer' out c

-- | Cancel last query sent to server
processInsertQuery ::
  -- | source
  (ZB.BufferedInput, ZB.BufferedOutput) ->
  Context ->
  -- | query without data
  Bytes ->
  -- | query id
  Maybe Bytes ->
  -- | data in Haskell type.
  [[ClickhouseType]] ->
  -- | return 1 if insertion successfully completed
  IO Bytes
processInsertQuery
  (i, o)
  ctx@Context{client_setting=client_setting, server_info=info}
  query_without_data
  query_id
  items = do
    sendQuery o info query_without_data query_id
    sendData o ctx "" Nothing
    sample_block <-
      ZB.readParser
        ( iterateWhile
            ( \case
                Block {..} -> False
                MultiString _ -> True
                Hello -> True
                x -> error ("unexpected packet type: " ++ show x)
            )
            $ receivePacket info
        )
        i
    case client_setting of
      Nothing -> error "empty client settings"
      Just ClientSetting {insert_block_size = siz} -> do
        let chunks = chunksOf (fromIntegral siz) items
        mapM_
          ( \chunk -> do
              let vectorized = List.transpose chunk
              let dataBlock = case sample_block of
                    Block
                      typeinfo@ColumnOrientedBlock
                        { columns_with_type = cwt
                        } ->
                        typeinfo
                          { cdata = vectorized
                          }
                    x -> error ("unexpected packet type: " ++ show x)
              sendData o ctx "" (Just dataBlock)
          )
          chunks
        sendData o ctx "" Nothing
        ZB.readParseChunk (P.parseChunk $ receivePacket info) i
        return "1"

-- | Read data from stream.
receiveData :: ServerInfo -> P.Parser Block.Block
receiveData info@ServerInfo {revision = revision} = do
  s <-
    if revision >= _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
      then readBinaryStr
      else return ""
  Block.readBlockInputStream info

-- | Transform received query data into Clickhouse type
receiveResult ::
  -- | Server information
  ServerInfo ->
  -- | Query information
  QueryInfo ->
  -- | Receive either error message or query result.
  P.Parser (Either String CKResult)
receiveResult info query_info = do
  packets <- packetGen
  let onlyDataPacket = filter isBlock packets
  let errors = (\(ErrorMessage str) -> str) <$> filter isError packets
  case errors of
    [] -> do
      let dataVectors = Block.cdata . queryData <$> onlyDataPacket
      let newQueryInfo = Prelude.foldl updateQueryInfo query_info packets
      return $ Right $ CKResult (concat dataVectors) newQueryInfo
    xs ->
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
      case d of
        (y : ys) : xs -> True
        _ -> False
    isBlock _ = False

    packetGen :: P.Parser [Packet]
    packetGen = do
      packet <- receivePacket info
      case packet of
        EndOfStream -> return []
        error@(ErrorMessage _) -> return [error]
        _ -> do
          next <- packetGen
          return (packet : next)

-- | Receive data packet from server
receivePacket :: ServerInfo -> P.Parser Packet
receivePacket info = do
  packet_type <- readVarInt
  -- The pattern matching does not support match with variable name,
  -- so here we use number instead.
  case packet_type of
    1 -> receiveData info <&> Block -- Data
    2 -> Error.readException Nothing <&> ErrorMessage . show -- Exception
    3 -> readProgress (revision info) <&> Progress -- Progress
    5 -> return EndOfStream -- End of Stream
    6 -> readBlockStreamProfileInfo <&> StreamProfileInfo --Profile
    7 -> receiveData info <&> Block -- Total
    8 -> receiveData info <&> Block -- Extreme
    -- 10 -> return undefined -- Log
    11 -> do
      -- MultiStrings message
      first <- readBinaryStr
      second <- readBinaryStr
      return $ MultiString (first, second)
    0 -> return Hello -- Hello
    _ ->
      error $
      show
        Error.ServerException
          { code = Error._UNKNOWN_PACKET_FROM_SERVER,
            message = "Unknown packet from server",
            nested = Nothing
          }

-- | write client information and server infomation to protocols
writeInfo ::
  ClientInfo ->
  ServerInfo ->
  B.Builder ()
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
