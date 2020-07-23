{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module ClickHouseDriver.IO.BufferedWriter (
    writeBinaryStr,
    writeVarUInt,
    c_write_varint,
    writeBinaryInt8,
    writeBinaryInt16,
    writeBinaryInt32,
    writeBinaryInt64,
    writeBinaryUInt8
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
import           Data.Word
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

writeBinaryUInt8 :: Word8->Builder->IO(Builder)
writeBinaryUInt8 w8 head = do
    let bytes = Binary.encode w8
    return $ head <> lazyByteString bytes

writeBinaryInt8 :: Int8->Builder->IO(Builder)
writeBinaryInt8 i8 head = do
    let bytes = Binary.encode i8
    return $ head <> lazyByteString bytes

writeBinaryInt16 :: Int16->Builder->IO(Builder)
writeBinaryInt16 i16 head = do
    let bytes = Binary.encode i16
    return $ head <> lazyByteString bytes

writeBinaryInt32 :: Int32->Builder->IO Builder
writeBinaryInt32 i32 head = do
    let bytes = Binary.encode i32
    return $ head <> lazyByteString bytes

writeBinaryInt64 :: Int64->Builder->IO(Builder)
writeBinaryInt64 i64 head = do
    let bytes = Binary.encode i64
    return $ head <> lazyByteString bytes
    
foreign import ccall unsafe "varuint.h write_varint" c_write_varint :: Word->IO (CString)