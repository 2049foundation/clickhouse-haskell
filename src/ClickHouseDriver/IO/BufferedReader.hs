-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings        #-}

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
    readBinaryUInt128,
    readBinaryUInt64,
    readBinaryUInt32,
    readBinaryUInt16,
    Reader,
    Buffer(..),
    createBuffer,
    refill
  )
where

import Control.Monad.State.Lazy ( StateT(StateT) )
import Data.Binary
    ( Word8, Word16, Word32, Word64, Binary, decode )
import           Data.Bits
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Char8    as C8
import qualified Data.ByteString.Lazy     as L
import qualified Data.ByteString.Unsafe   as UBS
import           Data.DoubleWord          (Word128 (..))
import           Data.Int
import           Data.Maybe
import qualified Data.Vector              as V
import           Data.Word
import           Foreign.C
import qualified Network.Simple.TCP       as TCP
import           Network.Socket           hiding (socket)

data Buffer = Buffer {
  bufSize :: !Int,
  bytesData :: ByteString,
  socket :: Maybe Socket
}

createBuffer :: Int->Socket->IO Buffer
createBuffer size sock = do
  receive <- TCP.recv sock size
  return Buffer{
    bufSize = size,
    bytesData = if isNothing receive then "" else fromJust receive,
    socket = Just sock
  }


refill :: Buffer->IO Buffer
refill Buffer{socket = Just sock, bufSize = size} = do
  newData' <- TCP.recv sock size
  let newBuffer = case newData' of
        Just newData -> Buffer {
          bufSize = size,
          bytesData = newData,
          socket = Just sock
        }
        Nothing -> error "Network error"
  return newBuffer
refill Buffer{socket=Nothing} = error "empty socket"

type Reader a = StateT Buffer IO a

readBinaryStrWithLength' :: Int -> Buffer -> IO (ByteString, Buffer)
readBinaryStrWithLength' n buf@Buffer{bufSize=size, bytesData=str, socket=sock} = do
  let l = BS.length str
  let (part, tail) = BS.splitAt n str
  if n > l
    then do
      newbuff <- refill buf
      (unread, altbuff) <- readBinaryStrWithLength' (n - l) newbuff
      return (part <> unread, altbuff)
    else do
      return (part, Buffer size tail sock)

readVarInt' :: Buffer -> IO (Word, Buffer)
readVarInt' buf@Buffer{bufSize=size,bytesData=str, socket=sock} = do
  let l = fromIntegral $ BS.length str
  skip <- UBS.unsafeUseAsCString str (\x -> c_count x l)
  if skip == 0
    then do
      varint' <- UBS.unsafeUseAsCString str (\x->c_read_varint 0 x l)
      new_buf <- refill buf
      let new_str = bytesData new_buf
      varint <- UBS.unsafeUseAsCString new_str (\x->c_read_varint varint' x l)
      skip2 <- UBS.unsafeUseAsCString new_str (\x->c_count x l)
      let tail = BS.drop (fromIntegral skip) new_str
      return (varint, Buffer size tail sock)
    else do
      varint <- UBS.unsafeUseAsCString str (\x -> c_read_varint 0 x l)
      let tail = BS.drop (fromIntegral skip) str
      return (varint, Buffer size tail sock)


readBinaryStr' :: Buffer -> IO (ByteString, Buffer)
readBinaryStr' str = do
  (len, tail) <- readVarInt' str
  (head, tail') <- readBinaryStrWithLength' (fromIntegral len) tail
  
  return (head, tail')

readBinaryHelper :: Binary a => Int -> Buffer -> IO (a, Buffer)
readBinaryHelper fmt str = do
  (cut, tail) <- readBinaryStrWithLength' fmt str
  let v = decode ((L.fromStrict. BS.reverse) cut)
  return (v, tail)


class Readable a where
  readIn :: Reader a

instance Readable Word where
  readIn = StateT readVarInt'

instance Readable ByteString where
  readIn = StateT readBinaryStr'

instance Readable Int8 where
  readIn = StateT $ readBinaryHelper 1

instance Readable Int16 where
  readIn = StateT $ readBinaryHelper 2

instance Readable Int32 where
  readIn = StateT $ readBinaryHelper 4

instance Readable Int64 where
  readIn = StateT $ readBinaryHelper 8

instance Readable Word8 where
  readIn = StateT $ readBinaryHelper 1

instance Readable Word16 where
  readIn = StateT $ readBinaryHelper 2

instance Readable Word32 where
  readIn = StateT $ readBinaryHelper 4

instance Readable Word64 where
  readIn = StateT $ readBinaryHelper 8

readVarInt :: Reader Word
readVarInt = readIn

readBinaryStrWithLength :: Int->Reader ByteString
readBinaryStrWithLength n = StateT (readBinaryStrWithLength' $ fromIntegral n)

readBinaryStr :: Reader ByteString
readBinaryStr = readIn

readBinaryInt8 :: Reader Int8
readBinaryInt8 = readIn

readBinaryInt16 :: Reader Int16
readBinaryInt16 = readIn

readBinaryInt32 :: Reader Int32
readBinaryInt32 = readIn

readBinaryInt64 :: Reader Int64
readBinaryInt64 = readIn

readBinaryUInt32 :: Reader Word32
readBinaryUInt32 = readIn

readBinaryUInt8 :: Reader Word8
readBinaryUInt8 = readIn

readBinaryUInt16 :: Reader Word16
readBinaryUInt16 = readIn

readBinaryUInt64 :: Reader Word64
readBinaryUInt64 = readIn

readBinaryUInt128 :: Reader Word128
readBinaryUInt128 = do
  hi <- readBinaryUInt64
  lo <- readBinaryUInt64
  return $ Word128 hi lo

foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: Word->CString -> Word -> IO Word

foreign import ccall unsafe "varuint.h count_read" c_count :: CString -> Word -> IO Word