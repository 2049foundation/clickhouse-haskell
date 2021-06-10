-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE OverloadedStrings        #-}

-- | Tools to serialize data sent server. This module is for internal use only.

module Database.ClickHouseDriver.IO.BufferedWriter
  ( writeBinaryStr,
    writeBinaryFixedLengthStr,
    writeVarUInt,
    writeBinaryInt8,
    writeBinaryInt16,
    writeBinaryInt32,
    writeBinaryInt64,
    writeBinaryUInt8,
    writeBinaryUInt16,
    writeBinaryUInt32,
    writeBinaryUInt64,
    writeBinaryUInt128
  )
where

import Control.Monad.IO.Class ( MonadIO(liftIO) )
import           Control.Monad.Writer         (WriterT, tell)
import qualified Data.Binary                  as Binary
import           Data.DoubleWord              (Word128 (..))
import Data.Int ( Int8, Int16, Int32, Int64 )
import Data.Word ( Word8, Word16, Word32, Word64 )
import Foreign.C ( CString )
import qualified Z.Data.Builder as B
import Z.Data.Vector (Bytes)
import qualified Z.Data.Vector as Z
import Data.Bits ( Bits(unsafeShiftR, (.|.), (.&.)) )
import Control.Monad ( when, mapM) 

writeBinaryFixedLengthStr :: Word->Bytes->B.Builder ()
writeBinaryFixedLengthStr len str = do
  let l = fromIntegral $ Z.length str
  if len /= l
    then error "Error: the length of the given bytestring does not equal to the given length"
    else do
      B.bytes str

writeBinaryStr :: Bytes->B.Builder ()
writeBinaryStr str = do
  let l = Z.length str
  writeVarUInt (fromIntegral l)
  B.bytes str

writeVarUInt :: Word->B.Builder ()
writeVarUInt x = do
  let byte = x .&. 0x7F
  if x > 0x7f
    then B.word8 (fromIntegral (byte .|. 0x80))
    else B.word8 (fromIntegral byte)
  let x' = x `unsafeShiftR` 7
  when (x'/= 0) $ writeVarUInt x'

encodeLoop :: (Bits a, Integral a) => Int -> a -> B.Builder ()
encodeLoop 0 _ = return ()
encodeLoop n acc = do
    B.word8 $ fromIntegral (acc .&. 0xFF)
    encodeLoop (n - 1) (acc `unsafeShiftR` 8) 

writeBinaryUInt8 :: Word8->B.Builder ()
writeBinaryUInt8 = B.word8

writeBinaryInt8 :: Int8->B.Builder ()
writeBinaryInt8 = B.word8 . fromIntegral

writeBinaryInt16 :: Int16->B.Builder ()
writeBinaryInt16 = encodeLoop 2

writeBinaryInt32 :: Int32->B.Builder ()
writeBinaryInt32 = encodeLoop 4

writeBinaryInt64 :: Int64->B.Builder ()
writeBinaryInt64 = encodeLoop 8

writeBinaryUInt16 :: Word16->B.Builder ()
writeBinaryUInt16 = encodeLoop 2

writeBinaryUInt32 :: Word32->B.Builder ()
writeBinaryUInt32 = encodeLoop 4

writeBinaryUInt64 :: Word64->B.Builder ()
writeBinaryUInt64 = encodeLoop 8

writeBinaryUInt128 :: Word128->B.Builder ()
writeBinaryUInt128 (Word128 hi lo) = do
  writeBinaryUInt64 hi
  writeBinaryUInt64 lo