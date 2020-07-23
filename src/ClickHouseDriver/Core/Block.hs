module ClickHouseDriver.Core.Block (
    BlockInfo(..),
    writeInfo,
    readInfo,
    readBlockInputStream,
    Block(..),
    ClickhouseType(..),
    defaultBlockInfo
) where

import Data.Int
import Data.ByteString
import Control.Monad.State
import Data.ByteString.Builder
import ClickHouseDriver.IO.BufferedWriter
import ClickHouseDriver.IO.BufferedReader
import Data.Vector  (Vector)
import qualified Data.Vector as V
import Data.Word

data ClickhouseType = CInt Int | CString ByteString | CFixedString ByteString Word | CDate ByteString | CDateTime ByteString

data BlockInfo = Info {
    is_overflows :: Bool,
    bucket_num :: Int32
}

data Block = ColumnOrientedBlock {
    columns_with_type :: Vector (ByteString, ByteString),
    cdata :: Vector (Vector ByteString),
    info :: BlockInfo
}

defaultBlockInfo :: BlockInfo
defaultBlockInfo = Info{
    is_overflows = False,
    bucket_num = -1
}

writeInfo :: BlockInfo->Builder->IO(Builder)
writeInfo (Info is_overflows bucket_num) builder = do
    r <- writeVarUInt 1 builder
     >>= writeBinaryUInt8 (if is_overflows then 1 else 0)
     >>= writeVarUInt 2
     >>= writeBinaryInt32 bucket_num
     >>= writeVarUInt 0
    return r

readInfo ::BlockInfo->StateT ByteString IO BlockInfo
readInfo info@Info {is_overflows=io, bucket_num=bn} = do
    field_num <- readVarInt
    case field_num of
        0 -> return info
        1 -> do
            io' <- readBinaryUInt8
            readInfo Info{is_overflows=if io' == 0 then False else True, bucket_num=bn}
        2 -> do
            bn' <- readBinaryInt32
            readInfo Info{is_overflows=io, bucket_num=bn'}

readBlockInputStream :: StateT ByteString IO Block
readBlockInputStream = do
    let defaultInfo = Info {
        is_overflows = False,
        bucket_num = -1
    } -- TODO should have considered the revision
    info <- readInfo defaultInfo

    n_columns <- readVarInt
    n_rows <- readVarInt

    --(datas, names, types) 
    let loop :: Int->StateT ByteString IO (Vector ByteString, ByteString, ByteString)
        loop n = do
            column_name <- readBinaryStr
            column_type <- readBinaryStr

            column <- readColumn column_type n_rows

            return (column, column_name, column_type)

    v <- V.generateM (fromIntegral n_columns) loop

    let datas = fmap (\(x,_,_)->x) v
        names = fmap (\(_,x,_)->x) v
        types = fmap (\(_,_,x)->x) v

    return ColumnOrientedBlock {
        cdata = datas,
        info = info,
        columns_with_type = V.zip names types
    }

readColumn :: ByteString->Word16->StateT ByteString IO (Vector ByteString)
readColumn coltype rows = do
    state_prefix <- readBinaryUInt64
    return undefined