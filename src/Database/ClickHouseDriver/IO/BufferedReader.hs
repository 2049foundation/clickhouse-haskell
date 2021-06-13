 -- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE BangPatterns             #-}
-- | Tools to analyze protocol and deserialize data sent from server. This module is for internal use only.

module Database.ClickHouseDriver.IO.BufferedReader
  ( readBinaryStr,
    readVarInt,
    readVarUInt,
    readBinaryInt8,
    readBinaryInt16,
    readBinaryInt64,
    readBinaryInt32,
    readBinaryUInt8,
    readBinaryUInt128,
    readBinaryUInt64,
    readBinaryUInt32,
    readBinaryUInt16,
    P.Parser,
    Buffer(..)
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
import Data.Bits ( Bits(shiftR, (.&.), (.|.), shiftL, unsafeShiftL) )
import Z.Data.Vector.Base (Bytes)
import qualified Z.Data.Vector.Base as Z
import qualified Z.Data.Vector as Z
import qualified Z.Data.Parser as P
import qualified Z.Foreign as Z
import Control.Monad ( forM, replicateM )
import Control.Monad.Loops (iterateM_)
import Data.List (scanl', foldl', span)
import qualified Streaming.Prelude as S

import Debug.Trace (trace)


(.$) :: a -> (a -> c) -> c
(.$) = flip ($)

-- | Buffer is for receiving data from TCP stream. Whenever all bytes are read, it automatically
-- refill from the stream.
data Buffer = Buffer {
  bufSize :: !Int,
  bytesData :: Bytes,
  socket :: Maybe Socket
}


loopDecodeNum :: (Num a, Bits a)=> Int -> P.Parser a
loopDecodeNum n = loopDecodeNum' n 0 0
  where
    loopDecodeNum' :: (Num a, Bits a)=> Int -> Int -> a -> P.Parser a
    loopDecodeNum' bit n ans
      | n == bit = return ans
      | otherwise = do
        byte <- P.satisfy $ const True
        let ans' = ans .|. (fromIntegral byte .&. 0xFF) `unsafeShiftL` (8 * n)
        loopDecodeNum' bit (n + 1) ans'

myList :: [(Int, Word)]
myList = (0 :: Int, 0 :: Word) : ((\(x, y)->(x + 1,y + 2)) <$> myList)

readVarUInt :: (S.Stream (S.Of Word) P.Parser Word)
readVarUInt = do
  let init = return [10..20]
  nums <- S.mapM (\x -> fromIntegral 
    <$> P.satisfy (const True)) init
  let res = takeUntil (\byte -> byte .&. 0x80 > 0) nums
  let res' = zip [0..8 :: Int] res
      final = foldl (\ans (i, byte)
        -> ans .|. (byte .&. 0x7F)
        `unsafeShiftL` (7 * i) ) 0 res'
  return final
  where
    takeUntil :: (a -> Bool) -> [a] -> [a]
    takeUntil f [] = []
    takeUntil f (x : xs)
        | f x = x : takeUntil f xs
        | otherwise = [x]


readVarInt :: P.Parser Word
readVarInt = loop 0 0
  where
    loop :: Int -> Word -> P.Parser Word
    loop !i !x
      | i >= 9 = return x
      | otherwise = do
          byte <- P.satisfy $ const True
          let x' = x .|. ((fromIntegral byte .&. 0x7F) `unsafeShiftL` (7 * i))
          if (byte .&. 0x80) > 0
            then loop (i + 1) x'
            else return x'

readBinaryStr :: P.Parser Bytes
readBinaryStr = do
  size <- readVarInt
  P.take $ fromIntegral size

readBinaryInt8 :: P.Parser Int8
readBinaryInt8 = loopDecodeNum 1

readBinaryInt16 :: P.Parser Int16
readBinaryInt16 = loopDecodeNum 2

readBinaryInt32 :: P.Parser Int32
readBinaryInt32 = loopDecodeNum 4

readBinaryInt64 :: P.Parser Int64
readBinaryInt64 = loopDecodeNum 8

readBinaryUInt32 :: P.Parser Word32
readBinaryUInt32 = loopDecodeNum 4

readBinaryUInt8 :: P.Parser Word8
readBinaryUInt8 = loopDecodeNum 1

readBinaryUInt16 :: P.Parser Word16
readBinaryUInt16 = loopDecodeNum 2

readBinaryUInt64 :: P.Parser Word64
readBinaryUInt64 = loopDecodeNum 8

readBinaryUInt128 :: P.Parser Word128
readBinaryUInt128 = do
  hi <- readBinaryUInt64
  lo <- readBinaryUInt64
  return $ Word128 hi lo
