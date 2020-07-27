{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.IO.BufferedReader
  ( readBinaryStrWithLength,
    readVarInt',
    readBinaryStr',
    readBinaryStr,
    readVarInt,
    readBinaryInt32,
    readBinaryUInt8,
    readBinaryUInt64,
    Reader
  )
where

import Control.Monad.State.Lazy
import Data.Binary
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Unsafe as UBS
import Data.Int
import Data.Word
import Foreign.C

type Buffer = ByteString

type Reader a = StateT Buffer IO a

-- TODO : Need to take into account the cases in which the reader hit the buffer. 

readBinaryStrWithLength :: Int -> ByteString -> IO (ByteString, ByteString)
readBinaryStrWithLength n str = return $ BS.splitAt n str

readVarInt' :: ByteString -> IO (Word, ByteString)
readVarInt' str = do
  let l = fromIntegral $ BS.length str
  varint <- UBS.unsafeUseAsCString str (\x -> c_read_varint x l)
  skip <- UBS.unsafeUseAsCString str (\x -> c_count x l)
  let tail = BS.drop (fromIntegral skip) str
  return (varint, tail)

readBinaryStr' :: ByteString -> IO (ByteString, ByteString)
readBinaryStr' str = do
  (len, tail) <- readVarInt' str
  (head, tail') <- readBinaryStrWithLength (fromIntegral len) tail
  return (head, tail')

readBinaryHelper :: Binary a => Int -> ByteString -> IO (a, ByteString)
readBinaryHelper fmt str = do
  (cut, tail) <- readBinaryStrWithLength fmt str
  let v = decode (L.fromStrict cut)
  return (v, tail)

readVarInt :: Reader Word
readVarInt = StateT readVarInt'

readBinaryStr :: Reader ByteString
readBinaryStr = StateT readBinaryStr'

readBinaryInt8 :: Reader Int8
readBinaryInt8 = StateT $ readBinaryHelper 1

readBinaryInt16 :: Reader Int8
readBinaryInt16 = StateT $ readBinaryHelper 2

readBinaryInt32 :: Reader Int32
readBinaryInt32 = StateT $ readBinaryHelper 4

readBinaryInt64 :: Reader Word64
readBinaryInt64 = StateT $ readBinaryHelper 8

readBinaryUInt32 :: Reader Int8
readBinaryUInt32 = StateT $ readBinaryHelper 4

readBinaryUInt8 :: Reader Word8
readBinaryUInt8 = StateT $ readBinaryHelper 1

readBinaryUInt16 :: Reader Int8
readBinaryUInt16 = StateT $ readBinaryHelper 2

readBinaryUInt64 :: Reader Word64
readBinaryUInt64 = StateT $ readBinaryHelper 8

foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: CString -> Word -> IO Word

foreign import ccall unsafe "varuint.h count_read" c_count :: CString -> Word -> IO Word