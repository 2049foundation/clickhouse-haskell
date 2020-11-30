-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

----------------------------------------------------------------------


{-# LANGUAGE FlexibleContexts #-}

-- | This module provides functions for handling data streaming communications.
--   For internal use only.
-- 

module Database.ClickHouseDriver.Core.Block
  ( BlockInfo (..),
    writeInfo,
    readInfo,
    readBlockInputStream,
    Block (..),
    defaultBlockInfo,
    writeBlockOutputStream,
    defaultBlock
  )
where

import Database.ClickHouseDriver.Core.Column
    ( ClickhouseType, readColumn, writeColumn )
import Database.ClickHouseDriver.Core.Defines as Defines
    ( _DBMS_MIN_REVISION_WITH_BLOCK_INFO )
import Database.ClickHouseDriver.Core.Types
    ( writeBlockInfo,
      Block(..),
      BlockInfo(..),
      Context(Context),
      ServerInfo(revision) )
import Database.ClickHouseDriver.IO.BufferedReader
    ( readBinaryInt32,
      readBinaryStr,
      readBinaryUInt8,
      readVarInt,
      Reader )
import Database.ClickHouseDriver.IO.BufferedWriter
    ( writeBinaryInt32,
      writeBinaryStr,
      writeBinaryUInt8,
      writeVarUInt,
      Writer )
import Data.ByteString ( ByteString )
import Data.ByteString.Builder ( Builder )
import           Data.Vector                        (Vector)
import           Data.Vector                        ((!))
import qualified Data.Vector                        as V
--Debug
--import           Debug.Trace

defaultBlockInfo :: BlockInfo
defaultBlockInfo =
  Info
    { is_overflows = False,
      bucket_num = -1
    }

defaultBlock :: Block
defaultBlock =
   ColumnOrientedBlock {
     columns_with_type=V.empty,
     cdata = V.empty,
     info = defaultBlockInfo
   }

-- | write block informamtion to string builder
writeInfo :: BlockInfo->Writer Builder
writeInfo (Info is_overflows bucket_num) = do
    writeVarUInt 1
    writeBinaryUInt8 (if is_overflows then 1 else 0)
    writeVarUInt 2
    writeBinaryInt32 bucket_num
    writeVarUInt 0

-- | read information from block information
readInfo :: BlockInfo -> Reader BlockInfo
readInfo info@Info {is_overflows = io, bucket_num = bn} = do
  field_num <- readVarInt
  case field_num of
    1 -> do
      io' <- readBinaryUInt8
      readInfo Info {is_overflows = if io' == 0 then False else True, bucket_num = bn}
    2 -> do
      bn' <- readBinaryInt32
      readInfo Info {is_overflows = io, bucket_num = bn'}
    _ -> return info

-- | Read a stream of data into a block. Data are read into column type
readBlockInputStream :: ServerInfo->Reader Block
readBlockInputStream server_info = do
  let defaultInfo =
        Info
          { is_overflows = False,
            bucket_num = -1
          } -- TODO should have considered the revision
  info <- readInfo defaultInfo
  n_columns <- readVarInt
  n_rows <- readVarInt
  let loop :: Int -> Reader (Vector ClickhouseType, ByteString, ByteString)
      loop n = do
        column_name <- readBinaryStr
        column_type <- readBinaryStr
        column <- readColumn server_info (fromIntegral n_rows) column_type
        return (column, column_name, column_type)
  v <- V.generateM (fromIntegral n_columns) loop
  let datas = (\(x, _, _) -> x) <$> v
      names = (\(_, x, _) -> x) <$> v
      types = (\(_, _, x) -> x) <$> v
  return
    ColumnOrientedBlock
      { cdata = datas,
        info = info,
        columns_with_type = V.zip names types
      }

-- | write data from column type into string builder.
writeBlockOutputStream :: Context->Block->Writer Builder
writeBlockOutputStream ctx@(Context _ server_info _)
  (ColumnOrientedBlock columns_with_type cdata info) = do
  let revis = fromIntegral $
        revision $
        (case server_info of
        Nothing   -> error ""
        Just info -> info)
  if revis >= Defines._DBMS_MIN_REVISION_WITH_BLOCK_INFO
    then writeBlockInfo info
    else return ()
  let n_rows = fromIntegral $ V.length cdata
      n_columns = fromIntegral $ V.length (cdata ! 0)
  writeVarUInt n_rows
  writeVarUInt n_columns
  V.imapM_ (\i (col, t)->do
       writeBinaryStr col
       writeBinaryStr t
       writeColumn ctx col t (cdata ! i)
       ) columns_with_type