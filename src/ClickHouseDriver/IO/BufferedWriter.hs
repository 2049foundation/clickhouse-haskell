{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.IO.BufferedWriter
  ( writeBinaryStr,
    writeVarUInt,
    c_write_varint,
    writeBinaryInt8,
    writeBinaryInt16,
    writeBinaryInt32,
    writeBinaryInt64,
    writeBinaryUInt8,
    Writer
  )
where

import Control.Monad.State.Lazy
import qualified Data.Binary as Binary
import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Data.ByteString.Builder
import Data.ByteString.Lazy.Builder (lazyByteString)
import Data.ByteString.Unsafe
import Data.Int
import Data.Monoid
import Data.Word
import Data.Word8
import Foreign.C
import Foreign.Ptr

type Writer a = a->Builder->IO(Builder)

writeBinaryStr :: Writer ByteString
writeBinaryStr str builder = do
  let l = BS.length str
  wint <- writeVarUInt (fromIntegral l) builder
  return $ wint <> byteString str

writeVarUInt ::Writer Word
writeVarUInt 0 builder = do
  ostr' <- c_write_varint 0
  ostr <- unsafePackCStringLen (ostr', 1)
  return $ builder <> byteString ostr
writeVarUInt n builder = do
  ostr' <- c_write_varint n
  ostr <- unsafePackCString ostr'
  return $ builder <> byteString ostr

-- TODO : the results should be reversed

writeBinaryUInt8 :: Writer Word8 
writeBinaryUInt8 w8 head = do
  let bytes = Binary.encode w8
  return $ head <> lazyByteString bytes

writeBinaryInt8 :: Writer Int8
writeBinaryInt8 i8 head = do
  let bytes = Binary.encode i8
  return $ head <> lazyByteString bytes

writeBinaryInt16 :: Writer Int16
writeBinaryInt16 i16 head = do
  let bytes = Binary.encode i16
  return $ head <> lazyByteString bytes

writeBinaryInt32 :: Writer Int32
writeBinaryInt32 i32 head = do
  let bytes = Binary.encode i32
  return $ head <> lazyByteString bytes

writeBinaryInt64 :: Writer Int64
writeBinaryInt64 i64 head = do
  let bytes = Binary.encode i64
  return $ head <> lazyByteString bytes

foreign import ccall unsafe "varuint.h write_varint" c_write_varint :: Word -> IO (CString)