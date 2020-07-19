{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module ClickHouseDriver.IO.BufferedReader (
    readBinaryStrWithLength,
    readVarInt',
    readBinaryStr',
    readBinaryStr,
    readVarInt
) where

import qualified Data.ByteString as BS
import Data.ByteString (ByteString)
import Foreign.C
import Data.Word
import qualified Data.ByteString.Unsafe    as UBS
import Control.Monad.State.Lazy

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

readVarInt :: StateT ByteString IO Word16
readVarInt = StateT readVarInt'

readBinaryStr :: StateT ByteString IO ByteString
readBinaryStr = StateT readBinaryStr'

foreign import ccall unsafe "varuint.h read_varint" c_read_varint :: CString->Word->IO Word16
foreign import ccall unsafe "varuint.h count_read"  c_count :: CString->Word->IO Word