# **连接**

clickhouse-haskell是clickhouse数据库服务器的用Haskell实现的客户端。客户端与服务器以bytestring的方式交互。
每次建立连接后，客户端会首先发送Hello给服务器。 等待服务器确认并返回Hello后，便可开始给服务器发送指令（如SE;ECT, INSERT）。 

# **通信协议**
每次从服务器查询数据传输到客户端后，需要将得到的ByteString解码，变换成本地的数据结构（ClickhouseType）。同样，每次向服务器插入数据，也需要将数据结构编码成bytestring再发送给服务器。

这里我们使用```StateT```实现字符串读取功能。```Buffer```就是这个```StateT```的状态。
在```BufferedReader.hs```文件中， 我们定义了```Buffer```类型，其功能是储存从服务端发过来的TCP流里的数据。
而
```Haskell
type Reader a = StateT Buffer IO a
```
用于分析一串未被解析的string的内容.

若想从原生string头部解析出一段可读字符串， 则可以使用```readBinaryStr```。 它首先将头部的几个字节解析成一个正整数n并将这几个字节删去，余下的string的头部就是所需要的可读字符串，而n表示的是这个字符串的长度，```readBinaryStr``` 就会根据这个长度来读取所有需要的字节。

由此可见，如果服务器发过来的是一些单词的话， 其应为, 打个比方 ```\length\word\length\word``` 的格式。

同样的方式，向服务器发送一些单词的时候，需要将每个单词的长度写在该单词的前面，然后再拼接起来相关实现参考```BufferedWriter.hs```。

# **序列化与反序列化协议**

虽然Clickhouse与SQL类似，但是它比SQL拥有更加丰富的数据类型，例如Array和Tuple。