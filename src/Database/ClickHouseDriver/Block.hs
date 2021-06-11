-- Copyright (c) 2020-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

----------------------------------------------------------------------


{-# LANGUAGE FlexibleContexts #-}

-- | This module provides functions for handling data streaming communications.
--   For internal use only.
-- 

module Database.ClickHouseDriver.Block
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

import Database.ClickHouseDriver.Column
    (readColumn, writeColumn )
import Database.ClickHouseDriver.Defines as Defines
    ( _DBMS_MIN_REVISION_WITH_BLOCK_INFO )
import Database.ClickHouseDriver.Types
    ( writeBlockInfo,
      Block(..),
      BlockInfo(..),
      Context(Context),
      ServerInfo(revision),
      ClickhouseType )
import Database.ClickHouseDriver.IO.BufferedReader
    ( readBinaryInt32,
      readBinaryStr,
      readBinaryUInt8,
      readVarInt,
    )
import Database.ClickHouseDriver.IO.BufferedWriter
    ( writeBinaryInt32,
      writeBinaryStr,
      writeBinaryUInt8,
      writeVarUInt,
    )

import Data.Vector ( Vector, (!) )
import qualified Data.Vector                        as V
import Control.Monad ( when, zipWithM_, forM)
--Debug
import           Debug.Trace
import Control.Monad.State.Lazy
import qualified Z.Data.Builder as B
import qualified Z.Data.Parser as P 
import Z.Data.Vector (Bytes)

defaultBlockInfo :: BlockInfo
defaultBlockInfo =
  Info
    { is_overflows = False,
      bucket_num = -1
    }

defaultBlock :: Block
defaultBlock =
   ColumnOrientedBlock {
     columns_with_type= [],
     cdata = [],
     info = defaultBlockInfo
   }

-- | write block informamtion to string builder
writeInfo :: BlockInfo->B.Builder ()
writeInfo (Info is_overflows bucket_num) = do
    writeVarUInt 1
    writeBinaryUInt8 (if is_overflows then 1 else 0)
    writeVarUInt 2
    writeBinaryInt32 bucket_num
    writeVarUInt 0

-- | read information from block information
readInfo :: BlockInfo -> P.Parser BlockInfo
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
readBlockInputStream :: ServerInfo->P.Parser Block
readBlockInputStream server_info = do
  let defaultInfo =
        Info
          { is_overflows = False,
            bucket_num = -1
          } -- TODO should have considered the revision
  info <- readInfo defaultInfo
  n_columns <- readVarInt
  n_rows <- readVarInt
  let loop :: P.Parser ([ClickhouseType], Bytes, Bytes)
      loop = do
        column_name <- readBinaryStr
        column_type <- readBinaryStr
        column <- readColumn server_info (fromIntegral n_rows) column_type
        return (column, column_name, column_type)
  let n_size = fromIntegral n_columns
  v <- replicateM n_size loop
  let datas = (\(x, _, _) -> x) <$> v
      names = (\(_, x, _) -> x) <$> v
      types = (\(_, _, x) -> x) <$> v
  return
    ColumnOrientedBlock
      { cdata = datas,
        info = info,
        columns_with_type = zip names types
      }

-- | write data from column type into string builder.
writeBlockOutputStream :: Context->Block->B.Builder ()
writeBlockOutputStream ctx@(Context _ server_info _)
  (ColumnOrientedBlock columns_with_type cdata info) = do
  let revis = fromIntegral $
        revision $
        (case server_info of
        Nothing   -> error ""
        Just info -> info)
  when (revis >= Defines._DBMS_MIN_REVISION_WITH_BLOCK_INFO) $ writeBlockInfo info
  let n_rows = fromIntegral $ length cdata
      n_columns = fromIntegral $ length $ head cdata
  writeVarUInt n_rows
  writeVarUInt n_columns
  zipWithM_ (
        \(col_name, type_name) xs->do
          writeBinaryStr col_name
          writeBinaryStr type_name
          writeColumn ctx col_name type_name xs
      ) columns_with_type cdata