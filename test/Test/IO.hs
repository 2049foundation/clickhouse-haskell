module Test.IO where

import ClickHouseDriver.IO.BufferedWriter
import ClickHouseDriver.IO.BufferedReader
import Test.QuickCheck
import Test.QuickCheck.Monadic
import qualified Data.ByteString as BS
import Control.Monad.Writer hiding (Writer)
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Int
import qualified Data.ByteString.Char8 as C8

prop_BinaryStr :: String->Property
prop_BinaryStr xs = monadicIO $ do
    let wtr = writeBinaryStr :: ByteString->Writer ByteString
    let packed = C8.pack xs
    a <- run (execWriterT $ wtr packed)
    let buf = Buffer {
        bufSize = BS.length a,
        socket = Nothing,
        bytesData = a
    }
    (res,_) <- run (runStateT readBinaryStr buf)
    assert (res == packed)

prop_Strs :: [String]->Property
prop_Strs xs = monadicIO $ do
    let wtr = writeBinaryStr :: ByteString->Writer ByteString
    let packed = fmap C8.pack xs 
    a <- run (execWriterT $ (mapM_ wtr packed))
    let buf = Buffer {
        bufSize = BS.length a,
        socket = Nothing,
        bytesData = a
    }
    (res, _) <- run (runStateT (mapM (\x->readBinaryStr) xs) buf)
    assert (res == packed)

prop_int8 :: [Int8]->Property
prop_int8 xs = monadicIO $ do
    let wtr = writeBinaryInt8 :: Int8->Writer ByteString
    a <- run (execWriterT $ (mapM_ wtr xs))
    let buf = Buffer {
        bufSize = BS.length a,
        socket = Nothing,
        bytesData = a
    }
    (res, _) <- run (runStateT (mapM (\x->readBinaryInt8) xs) buf)
    assert (res == xs)

prop_fix_str :: [String]->Property
prop_fix_str xs = monadicIO $ do
    let wtr = writeBinaryFixedLengthStr :: Word->ByteString->Writer ByteString
    let packed = fmap C8.pack xs
    a <- run $ execWriterT $ (mapM_ (\str->wtr (fromIntegral $ BS.length str) str ) packed)
    let buf = Buffer {
        bufSize = BS.length a,
        socket = Nothing,
        bytesData = a
    }
    (res,_) <- run $ runStateT (mapM (\str->readBinaryStrWithLength (BS.length str)) packed) buf
    assert (res == packed)

runTests = do
    print "test"
    quickCheck prop_BinaryStr
    quickCheck prop_int8
    quickCheck prop_Strs
    quickCheck prop_fix_str