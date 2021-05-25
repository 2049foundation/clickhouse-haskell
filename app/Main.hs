{-# LANGUAGE OverloadedStrings #-}
import Database.ClickHouseDriver.IO.BufferedWriter
import Database.ClickHouseDriver.IO.BufferedReader
import Test.QuickCheck
import Test.QuickCheck.Monadic
import qualified Data.ByteString as BS
import Control.Monad.Writer hiding (Writer)
import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Int
import qualified Data.ByteString.Char8 as C8
import Database.ClickHouseDriver

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

prop_int32 :: [Int32]->Property
prop_int32 xs = monadicIO $ do
    let wtr = writeBinaryInt32 :: Int32->Writer ByteString
    a <- run (execWriterT $ (mapM_ wtr xs))
    let buf = Buffer {
        bufSize = BS.length a,
        socket = Nothing,
        bytesData = a
    }
    (res, _) <- run (runStateT (mapM (\x->readBinaryInt32) xs) buf)
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
    quickCheck prop_int32
    quickCheck prop_Strs
    quickCheck prop_fix_str

--main :: IO () 
--main = do
 -- runTests

main :: IO ()
main = do
    conn <- defaultClient
    ping conn
    --query conn ("CREATE TABLE IF NOT EXISTS big " ++ 
     --       "(`str` String, `int` Int16, `fix` FixedString(3))" ++ "ENGINE = Memory")
    insertOneRow conn "INSERT INTO big VALUES" [CKString "cacd", CKInt16 16, CKString "124"]
    q <- query conn "SELECT * FROM big"
    print q