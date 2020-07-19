{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module ClickHouseDriver.IO.BufferedWriter (
    writeBinaryStr,
    writeVarUInt,
    c_write_varint,
) where

import           Data.Word8
import qualified Data.ByteString as BS
import           Data.ByteString  (ByteString)
import           Data.ByteString.Unsafe
import           Data.ByteString.Builder
import           Data.Monoid
import           Foreign.C
import           Foreign.Ptr
import           Control.Monad.State.Lazy


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




{-
writeSettings :: ByteString->Bool->Builder->IO(Builder)
writeSettings setting settings_as_strings buf = do
    let is_important = 0
-}

    



foreign import ccall unsafe "varuint.h write_varint" c_write_varint :: Word->IO (CString)