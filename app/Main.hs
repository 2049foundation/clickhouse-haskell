{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module Main where

import           ClickHouseDriver
import           Control.Monad.ST
import           Haxl.Core
import           Haxl.Core.Monad
import           Data.Text
import           Network.HTTP.Client
import           Data.ByteString        
import           Data.ByteString.Char8
import           Foreign.C
import           ClickHouseDriver.IO.BufferedWriter
import           ClickHouseDriver.IO.BufferedReader
import           Data.Monoid
import           Control.Monad.State.Lazy
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Word
import           Network.Socket                                           
import qualified Network.Simple.TCP                        as TCP
import qualified Data.Binary as B
import           Data.Int
import           Control.Monad.Writer
import qualified Data.Vector as V
import           Control.Monad.Loops






readResult :: StateT ByteString IO Word
readResult = do
    r <- readVarInt
    r2 <- readVarInt
    r3 <- readVarInt
    return r3

readResult2 :: StateT ByteString IO ByteString
readResult2 = do
    r <- readBinaryStr
    r' <- readBinaryStr
    return (r <> r')

manualTCP :: IO()
manualTCP = do
    print "manual"
    conn <- defaultTCPConnection
    case conn of
        Left e -> print e
        Right settings->do
            print "connected"
            sendQuery "SHOW DATABASES" Nothing settings
            sendData "" settings
            case settings of
                TCPConnection {tcpSocket=sock}-> do
                    x <- TCP.recv sock 2048
                    print x
                    y <- TCP.recv sock 2048
                    print y
                    z <- TCP.recv sock 2048
                    print z
                    TCP.closeSock sock
                    
                    
                
main3 :: IO()
main3 = do
    (sock, sockaddr) <- TCP.connectSock "localhost" "9000"
    print sock


query :: IO()
query = do
    conn <- defaultHttpConnection
    env <- setupEnv conn
    res <- runQuery env (getJSON "SHOW DATABASES")
    print res


class MyType a

instance MyType Int

instance MyType Char

instance MyType Bool

myfunc :: (MyType a)=>a->a
myfunc a = a

testByteString :: IO ByteString
testByteString = do
    (_,res) <- runWriterT $ do
        writeBinaryStr "hello"
        writeBinaryStr "world"
        writeBinaryStr "my"
        writeBinaryStr "darling"
    return res

readBinaryStrN = V.replicateM 4 readBinaryStr

testIntStr :: IO ByteString
testIntStr = do
    (_,res) <- runWriterT $ do
        writeVarUInt 2
        writeVarUInt 4
        writeVarUInt 8
        writeVarUInt 10
        writeVarUInt 12
    return res


readManyVarInt :: Reader [Word]
readManyVarInt = do
    int <- readVarInt
    if int == 10 
        then
        return [10]
        else do
            next <- readManyVarInt
            return (int : next)
    

main :: IO()
main = do
    print "Test Section"
    conn <- defaultTCPConnection
    env <- client conn
    print "connected"
    res <- execute "SHOW DATABASES" env
    print res