{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module ClickHouseDriver.IO.BufferedReader (
    readBinaryStrWithLength,
    readVarInt',
    readBinaryStr',
    readBinaryStr,
    readVarInt,
    readBinaryInt32,
    readBinaryUInt8
) where

import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Foreign.C
import Data.Word
import qualified Data.ByteString.Unsafe    as UBS
import Control.Monad.State.Lazy
import Data.Int
import Data.Binary   (decode)
import qualified Data.ByteString.Lazy   as L

type Buffer = ByteString

readBinaryStrWithLength :: Int->ByteString->IO(ByteString, ByteString)
readBinaryStrWithLength n str = return $ BS.splitAt n str

readVarInt' :: ByteString->IO (Word16, ByteString)
readVarInt' str = do
    let l = fromIntegral $ BS.length str
    varint <- UBS.unsafeUseAsCString str (\x->c_read_varint x l)
    skip <- UBS.unsafeUseAsCString str (\x->c_count x l)
    let tail = BS.drop (fromIntegral skip) str 
    return (varint, tail)

readBinaryStr' :: ByteString->IO(ByteString, ByteString)
readBinaryStr' str = do
    (len, tail) <- readVarInt' str
    (head, tail') <- readBinaryStrWithLength (fromIntegral len) tail
    return (head, tail')

readBinaryInt32' :: ByteString->IO(Int32, ByteString)
readBinaryInt32' str = do
    (cut, tail) <- readBinaryStrWithLength 4 str
    let i32 = decode (L.fromStrict cut)
    return (i32, tail)

readBinaryUInt8' :: ByteString->IO(Word8, ByteString)
readBinaryUInt8' str = do
    (cut, tail) <- readBinaryStrWithLength 1 str
    let u8 = decode (L.fromStrict cut)
    return (u8, tail)

readVarInt :: StateT ByteString IO Word16
readVarInt = StateT readVarInt'

readBinaryStr :: StateT ByteString IO ByteString
readBinaryStr = StateT readBinaryStr'

readBinaryInt32 :: StateT ByteString IO Int32
readBinaryInt32 = StateT readBinaryInt32'

readBinaryUInt8 :: StateT ByteString IO Word8
readBinaryUInt8 = StateT readBinaryUInt8'

foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: CString->Word->IO Word16
foreign import ccall unsafe "varuint.h count_read"  c_count :: CString->Word->IO Word