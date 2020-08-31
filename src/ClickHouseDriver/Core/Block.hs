{-# LANGUAGE FlexibleContexts #-}

module ClickHouseDriver.Core.Block
  ( BlockInfo (..),
    writeInfo,
    readInfo,
    readBlockInputStream,
    Block (..),
    defaultBlockInfo,
    writeBlockOutputStream
  )
where

import ClickHouseDriver.IO.BufferedReader
import ClickHouseDriver.IO.BufferedWriter
import Control.Monad.State
import Data.ByteString
import Data.ByteString.Builder
import Data.Int
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word
import ClickHouseDriver.Core.Column
import qualified Data.List as List
import ClickHouseDriver.Core.Types
import ClickHouseDriver.Core.Defines as Defines
import Data.Vector ((!))
--Debug
import Debug.Trace

defaultBlockInfo :: BlockInfo
defaultBlockInfo =
  Info
    { is_overflows = False,
      bucket_num = -1
    }


writeInfo :: BlockInfo->IOWriter Builder
writeInfo (Info is_overflows bucket_num) = do
    writeVarUInt 1 
    writeBinaryUInt8 (if is_overflows then 1 else 0)
    writeVarUInt 2
    writeBinaryInt32 bucket_num
    writeVarUInt 0


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
    

readBlockInputStream :: Reader Block
readBlockInputStream = do
  let defaultInfo =
        Info
          { is_overflows = False,
            bucket_num = -1
          } -- TODO should have considered the revision
  info <- readInfo defaultInfo

  n_columns <- readVarInt
  n_rows <- readVarInt

  --(datas, names, types)
  let loop :: Int -> Reader (Vector ClickhouseType, ByteString, ByteString)
      loop n = do
        column_name <- readBinaryStr
        column_type <- readBinaryStr

        column <- readColumn (fromIntegral n_rows) column_type

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

writeBlockOutputStream :: Context->Block->IOWriter Builder
writeBlockOutputStream ctx@(Context client_info server_info client_settings) 
  (ColumnOrientedBlock columns_with_type cdata info) = do
  let revis = fromIntegral $ 
        revision $ 
        (case server_info of
        Nothing -> error ""
        Just info -> info)
  if revis >= Defines._DBMS_MIN_REVISION_WITH_BLOCK_INFO
    then writeBlockInfo info
    else return ()
  let n_rows = fromIntegral $ V.length cdata
      n_columns = fromIntegral $ V.length (cdata ! 0)
  writeVarUInt n_columns
  writeVarUInt n_rows
  V.imapM_ (\i (col, t)->do 
       writeBinaryStr col
       writeBinaryStr t
       writeColumn ctx col t (cdata ! i)
       ) columns_with_type