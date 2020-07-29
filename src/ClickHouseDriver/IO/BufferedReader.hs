{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.IO.BufferedReader
  ( readBinaryStrWithLength,
    readVarInt',
    readBinaryStr',
    readBinaryStr,
    readVarInt,
    readBinaryInt8,
    readBinaryInt16,
    readBinaryInt64,
    readBinaryInt32,
    readBinaryUInt8,
    readBinaryUInt64,
    readBinaryUInt32,
    readBinaryUInt16,
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
import qualified Data.Vector as V

type Buffer = ByteString

type Reader a = StateT Buffer IO a

-- TODO : Need to take into account the cases in which the reader hit the buffer. 

readBinaryStrWithLength' :: Int -> ByteString -> IO (ByteString, ByteString)
readBinaryStrWithLength' n str = return $ BS.splitAt n str

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
  (head, tail') <- readBinaryStrWithLength' (fromIntegral len) tail
  return (head, tail')

readBinaryHelper :: Binary a => Int -> ByteString -> IO (a, ByteString)
readBinaryHelper fmt str = do
  (cut, tail) <- readBinaryStrWithLength' fmt str
  let v = decode (L.fromStrict cut)
  return (v, tail)


class Readable a where
  readone :: Reader a

instance Readable Word where
  readone = StateT readVarInt'

instance Readable ByteString where
  readone = StateT readBinaryStr'

instance Readable Int8 where
  readone = StateT $ readBinaryHelper 1

instance Readable Int16 where
  readone = StateT $ readBinaryHelper 2

instance Readable Int32 where
  readone = StateT $ readBinaryHelper 4

instance Readable Int64 where
  readone = StateT $ readBinaryHelper 8

instance Readable Word8 where
  readone = StateT $ readBinaryHelper 1

instance Readable Word16 where
  readone = StateT $ readBinaryHelper 2

instance Readable Word32 where
  readone = StateT $ readBinaryHelper 4

instance Readable Word64 where
  readone = StateT $ readBinaryHelper 8

readVarInt :: Reader Word
readVarInt = readone

readBinaryStrWithLength :: Int->Reader ByteString
readBinaryStrWithLength n = StateT (readBinaryStrWithLength' $ fromIntegral n)

readBinaryStr :: Reader ByteString
readBinaryStr = readone

readBinaryInt8 :: Reader Int8
readBinaryInt8 = readone

readBinaryInt16 :: Reader Int16
readBinaryInt16 = readone

readBinaryInt32 :: Reader Int32
readBinaryInt32 = readone

readBinaryInt64 :: Reader Int64
readBinaryInt64 = readone

readBinaryUInt32 :: Reader Word32
readBinaryUInt32 = readone

readBinaryUInt8 :: Reader Word8
readBinaryUInt8 = readone

readBinaryUInt16 :: Reader Word16
readBinaryUInt16 = readone

readBinaryUInt64 :: Reader Word64
readBinaryUInt64 = readone

readNBiInt8 = V.replicateM 3 readBinaryStr


foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: CString -> Word -> IO Word

foreign import ccall unsafe "varuint.h count_read" c_count :: CString -> Word -> IO Word