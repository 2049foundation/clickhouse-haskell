{-# LANGUAGE FlexibleContexts #-}

module ClickHouseDriver.Core.Block
  ( BlockInfo (..),
    writeInfo,
    readInfo,
    readBlockInputStream,
    Block (..),
    ClickhouseType (..),
    defaultBlockInfo,
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

data ClickhouseType = CInt Int | CString ByteString | CFixedString ByteString Word | CDate ByteString | CDateTime ByteString

data BlockInfo = Info
  { is_overflows :: Bool,
    bucket_num :: Int32
  }

data Block = ColumnOrientedBlock
  { columns_with_type :: Vector (ByteString, ByteString),
    cdata :: Vector (Vector ByteString),
    info :: BlockInfo
  }

defaultBlockInfo :: BlockInfo
defaultBlockInfo =
  Info
    { is_overflows = False,
      bucket_num = -1
    }
--(MonoidMap ByteString w,MonoidMap Word8 w,MonoidMap Int32 w)=>
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
  let loop :: Int -> Reader (Vector ByteString, ByteString, ByteString)
      loop n = do
        column_name <- readBinaryStr
        column_type <- readBinaryStr

        column <- readColumn column_type n_rows

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

readColumn :: ByteString 
              -- ^ Column type
           -> Word
              -- ^ row size
           -> Reader (Vector ByteString)
readColumn coltype rows = do
  state_prefix <- readBinaryUInt64
  return undefined