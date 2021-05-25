 -- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings        #-}
-- | Tools to analyze protocol and deserialize data sent from server. This module is for internal use only.

module Database.ClickHouseDriver.IO.BufferedReader
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
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Lazy     as L
import qualified Data.ByteString.Unsafe   as UBS
import           Data.DoubleWord          (Word128 (..))
import Data.Int ( Int8, Int16, Int32, Int64 )
import Data.Maybe ( fromJust, isNothing, fromMaybe )
import Foreign.C ( CString )
import qualified Network.Simple.TCP       as TCP
import Network.Socket ( Socket )
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peek, peekElemOff)
import Data.Bits


-- | Buffer is for receiving data from TCP stream. Whenever all bytes are read, it automatically
-- refill from the stream.
data Buffer = Buffer {
  bufSize :: !Int,
  bytesData :: ByteString,
  socket :: Maybe Socket
}


bitMask :: Word32
{-# INLINE bitMask #-}
bitMask = 0xffff

-- | create buffer with size and socket.
createBuffer :: Int->Socket->IO Buffer
createBuffer size sock = do
  receive <- TCP.recv sock size -- receive data
  return Buffer{
    bufSize = size, -- set the size 
    bytesData = fromMaybe "" receive,
    socket = Just sock
  }

-- | refill buffer from stream
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

readBinaryStrWithLength' :: Int
                         -- ^ length of string
                         -> Buffer
                         -- ^ buffer to read
                         -> IO (ByteString, Buffer)
                         -- ^ (the string read from buffer, buffer after reading)
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

readVarInt' :: Buffer
              -- ^ buffer to be read
            -> IO (Word, Buffer)
              -- ^ (the word read from buffer, the buffer after reading)
readVarInt' buf@Buffer{bufSize=size,bytesData=str, socket=sock} = do
  let l = fromIntegral $ BS.length str
  n_cont <- UBS.unsafeUseAsCString str (\x -> c_read_varint 0 x l)
  let skip = n_cont .&. bitMask
  
  if skip == 0
    then do
      n_cont_2 <- UBS.unsafeUseAsCString str (\x->c_read_varint 0 x l)

      let varuint' = n_cont_2 `shiftR` 16
      new_buf <- refill buf
      let new_str = bytesData new_buf

      n_cont_3 <- UBS.unsafeUseAsCString new_str (\x->c_read_varint (fromIntegral varuint') x l)
      let skip2 = n_cont_3 .&. bitMask
          varuint = n_cont_3 `shiftR` 16
      let tail = BS.drop (fromIntegral skip2) new_str
      return (fromIntegral varuint, Buffer size tail sock)
    else do
      n_cont <- UBS.unsafeUseAsCString str (\x -> c_read_varint 0 x l)
      let skip2 = n_cont .&. bitMask
          varuint = n_cont `shiftR` 16

      let tail = BS.drop (fromIntegral skip) str
      return (fromIntegral varuint, Buffer size tail sock)

--readVarInt' :: Buffer
--              -- ^ buffer to be read
--            -> IO (Word, Buffer)
--              -- ^ (the word read from buffer, the buffer after reading)
--readVarInt' buf@Buffer{bufSize=size,bytesData=str, socket=sock} = do
--  let l = fromIntegral $ BS.length str
--  ptr <- UBS.unsafeUseAsCString str (\x -> c_read_varint 0 x l)
--  skip <- peek ptr
--  if skip == 0
--    then do
--      varuint_ptr <- UBS.unsafeUseAsCString str (\x->c_read_varint 0 x l)
--
--      varuint' <- peekElemOff varuint_ptr 1
--      new_buf <- refill buf
--      let new_str = bytesData new_buf
--
--      ptr2 <- UBS.unsafeUseAsCString new_str (\x->c_read_varint varuint' x l)
--      skip2 <- peek ptr2
--      varuint <- peekElemOff ptr2 1
--
--      let tail = BS.drop (fromIntegral skip2) new_str
--      return (varuint, Buffer size tail sock)
--    else do
--      ptr <- UBS.unsafeUseAsCString str (\x -> c_read_varint 0 x l)
--      skip2 <- peek ptr
--      varuint <- peekElemOff ptr 1
--
--      let tail = BS.drop (fromIntegral skip) str
--      return (varuint, Buffer size tail sock)
-- | read binary string from buffer.
-- It first read the integer(n) in front of the desired string,
-- then it read n bytes to capture the whole string.
readBinaryStr' :: Buffer
               -- ^ Buffer to be read
               -> IO (ByteString, Buffer)
               -- ^ (the string read from Buffer, the buffer after reading)
readBinaryStr' str = do
  (len, tail) <- readVarInt' str -- 
  (head, tail') <- readBinaryStrWithLength' (fromIntegral len) tail

  return (head, tail')

-- | read n bytes and then transform into a binary type such as bytestring, Int8, UInt16 etc.
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

-- | read bytes in the little endian format and transform into integer, see CBits/varuint.c
foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: Word->CString -> Word -> IO Word32

-- | Helper of c_read_varint. it counts how many bits it needs to read.   
-- foreign import ccall unsafe "varuint.h count_read" c_count :: CString -> Word -> IO Word