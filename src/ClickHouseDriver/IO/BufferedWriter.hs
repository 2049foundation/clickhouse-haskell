{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module ClickHouseDriver.IO.BufferedWriter (
    writeBinaryStr,
    writeVarUInt,
    c_write_varint,
) where

import           Data.Word8
import           Data.ByteString
import           Data.ByteString.Unsafe
import           Data.ByteString.Builder
import           Data.Monoid
import           Foreign.C
import           Foreign.Ptr

writeBinaryStr :: ByteString->Builder->IO Builder
writeBinaryStr str builder = return $ builder <> byteString str

writeVarUInt :: Word->Builder->IO Builder
writeVarUInt n builder = do
    ostr' <- c_write_varint n
    ostr  <- unsafePackCString ostr' 
    return $ builder <> byteString ostr

foreign import ccall unsafe "varuint.h write_varint" c_write_varint :: Word->IO (CString)
