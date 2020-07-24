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

readBinaryStrWithLength :: Int -> ByteString -> IO (ByteString, ByteString)
readBinaryStrWithLength n str = return $ BS.splitAt n str

readVarInt' :: ByteString -> IO (Word16, ByteString)
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

readVarInt :: Reader Word16
readVarInt = StateT readVarInt'

readBinaryStr :: Reader ByteString
readBinaryStr = StateT readBinaryStr'

readBinaryInt32 :: Reader Int32
readBinaryInt32 = StateT $ readBinaryHelper 4

readBinaryUInt8 :: Reader Word8
readBinaryUInt8 = StateT $ readBinaryHelper 1

readBinaryUInt64 :: Reader Word64
readBinaryUInt64 = StateT $ readBinaryHelper 8

foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: CString -> Word -> IO Word16

foreign import ccall unsafe "varuint.h count_read" c_count :: CString -> Word -> IO Word