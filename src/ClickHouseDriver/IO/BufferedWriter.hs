{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module ClickHouseDriver.IO.BufferedWriter (
    writeBinaryStr,
    writeVarUInt,
    c_write_varint,
    writeInt8Str,
    writeInt16Str,
    writeInt32Str,
    writeInt64Str
) where

import           Data.Word8
import qualified Data.ByteString as BS
import           Data.ByteString  (ByteString)
import           Data.ByteString.Unsafe
import           Data.ByteString.Lazy.Builder (lazyByteString)
import           Data.ByteString.Builder
import           Data.Monoid
import           Foreign.C
import           Foreign.Ptr
import           Control.Monad.State.Lazy
import           Data.Int
import qualified Data.Binary as Binary


writeBinaryStr :: ByteString->Builder->IO Builder
writeBinaryStr str builder = do
    let l = BS.length str
    wint <- writeVarUInt (fromIntegral l) builder 
    return $ wint <> byteString str

writeVarUInt :: Word->Builder->IO Builder
writeVarUInt 0 builder = do
    ostr' <- c_write_varint 0
    ostr <- unsafePackCStringLen (ostr', 1)
    return $ builder <> byteString ostr
writeVarUInt n builder = do
    ostr' <- c_write_varint n
    ostr  <- unsafePackCString ostr'         
    return $ builder <> byteString ostr

-- TODO : the results should be reversed

writeInt8Str :: Int8->Builder->IO(Builder)
writeInt8Str i8 head = do
    let bytes = Binary.encode i8
    return $ head <> lazyByteString bytes

writeInt16Str :: Int16->Builder->IO(Builder)
writeInt16Str i16 head = do
    let bytes = Binary.encode i16
    return $ head <> lazyByteString bytes

writeInt32Str :: Int32->Builder->IO Builder
writeInt32Str i32 head = do
    let bytes = Binary.encode i32
    return $ head <> lazyByteString bytes

writeInt64Str :: Int64->Builder->IO(Builder)
writeInt64Str i64 head = do
    let bytes = Binary.encode i64
    return $ head <> lazyByteString bytes
    
{-
writeSettings :: ByteString->Bool->Builder->IO(Builder)
writeSettings setting settings_as_strings buf = do
    let is_important = 0
-}

foreign import ccall unsafe "varuint.h write_varint" c_write_varint :: Word->IO (CString)