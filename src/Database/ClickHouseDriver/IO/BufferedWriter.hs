-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE OverloadedStrings        #-}

-- | Tools to serialize data sent server. This module is for internal use only.

module Database.ClickHouseDriver.IO.BufferedWriter
  ( writeBinaryStr,
    writeBinaryFixedLengthStr,
    writeVarUInt,
    c_write_varint,
    writeBinaryInt8,
    writeBinaryInt16,
    writeBinaryInt32,
    writeBinaryInt64,
    writeBinaryUInt8,
    writeBinaryUInt16,
    writeBinaryUInt32,
    writeBinaryUInt64,
    writeBinaryUInt128,
    writeIn,
    transform,
    Writer,
    MonoidMap,
  )
where

import Control.Monad.IO.Class ( MonadIO(liftIO) )
import           Control.Monad.Writer         (WriterT, tell)
import qualified Data.Binary                  as Binary
import           Data.ByteString              (ByteString)
import qualified Data.ByteString              as BS
import Data.ByteString.Builder
    ( Builder, toLazyByteString, byteString, lazyByteString)
import qualified Data.ByteString.Lazy         as L
import Data.ByteString.Unsafe
    ( unsafePackCString, unsafePackCStringLen )
import           Data.DoubleWord              (Word128 (..))
import Data.Int ( Int8, Int16, Int32, Int64 )
import Data.Word ( Word8, Word16, Word32, Word64 )
import Foreign.C ( CString )

-- Monoid Homomorphism.
class (Monoid w, Monoid m)=>MonoidMap w m where
  transform :: w->m

instance MonoidMap ByteString L.ByteString where
  transform = L.fromStrict

instance MonoidMap L.ByteString ByteString where
  transform = L.toStrict

instance MonoidMap ByteString Builder where
  transform = byteString

instance MonoidMap L.ByteString Builder where
  transform = lazyByteString

instance MonoidMap Builder L.ByteString where
  transform = toLazyByteString

instance MonoidMap Builder ByteString where
  transform = L.toStrict . toLazyByteString

instance (Monoid w)=>MonoidMap w w where
  transform = id

-- | The writer monad writes bytestring builders and combine them as a monoid. 
type Writer w = WriterT w IO ()

writeBinaryFixedLengthStr :: (MonoidMap ByteString w)=>Word->ByteString->Writer w
writeBinaryFixedLengthStr len str = do
  let l = fromIntegral $ BS.length str
  if len /= l
    then error "Error: the length of the given bytestring does not equal to the given length"
    else do
      writeIn str

writeBinaryStr :: (MonoidMap ByteString w)=>ByteString->Writer w
writeBinaryStr str = do
  let l = BS.length str
  writeVarUInt (fromIntegral l)
  writeIn str

writeVarUInt ::(MonoidMap ByteString w)=>Word->Writer w
writeVarUInt n = do
   varuint <- liftIO $ leb128 n
   writeIn varuint 
   where
      leb128 :: Word->IO ByteString
      leb128 0 = do
        ostr' <- c_write_varint 0
        unsafePackCStringLen (ostr', 1)
      leb128 n = do
        ostr' <- c_write_varint n
        unsafePackCString ostr'

writeBinaryUInt8 :: (MonoidMap L.ByteString w)=>Word8->Writer w
writeBinaryUInt8 = tell . transform . L.reverse . Binary.encode

writeBinaryInt8 :: (MonoidMap L.ByteString w)=>Int8->Writer w
writeBinaryInt8 = tell . transform . L.reverse . Binary.encode

writeBinaryInt16 :: (MonoidMap L.ByteString w)=>Int16->Writer w
writeBinaryInt16 = tell . transform . L.reverse . Binary.encode

writeBinaryInt32 :: (MonoidMap L.ByteString w)=>Int32->Writer w
writeBinaryInt32 = tell . transform . L.reverse . Binary.encode

writeBinaryInt64 :: (MonoidMap L.ByteString w)=>Int64->Writer w
writeBinaryInt64 = tell . transform . L.reverse . Binary.encode

writeBinaryUInt16 :: (MonoidMap L.ByteString w)=>Word16->Writer w
writeBinaryUInt16 = tell . transform . L.reverse . Binary.encode

writeBinaryUInt32 :: (MonoidMap L.ByteString w)=>Word32->Writer w
writeBinaryUInt32 = tell . transform . L.reverse . Binary.encode

writeBinaryUInt64 :: (MonoidMap L.ByteString w)=>Word64->Writer w
writeBinaryUInt64 = tell . transform . L.reverse . Binary.encode

writeBinaryUInt128 :: (MonoidMap L.ByteString w)=>Word128->Writer w
writeBinaryUInt128 (Word128 hi lo) = do
  writeBinaryUInt64 hi
  writeBinaryUInt64 lo

writeIn :: (MonoidMap m w)=>m->Writer w
writeIn = tell . transform

foreign import ccall unsafe "varuint.h write_varint" c_write_varint :: Word -> IO CString