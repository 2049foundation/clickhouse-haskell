<<<<<<< HEAD
# clickhouse-haskell

Haskell driver for ClickHouse
=======
# clickhouse-haskell-
This project is Haskell driver for ClickHouse.
## Example


Let's see the names of the databases in the clickhouse server.
```haskell
module Main where

import ClickHouseDriver

main :: IO()
main = do
    let deSetting = ClickHouseConnectionSettings {
        ciHost = "localhost",
        ciPassword = "",
        ciPort = 8123,
        ciUsername = "default"
    }
    env <- defaultEnv deSetting --set up environment (i.e. username, password etc.).
    result <- runQuery env (getJSON "SHOW DATABASES")
    print result

```
Outputs:
```
Right [fromList [("name",String "DEMO")],fromList [("name",String "_temporary_and_external_tables")],fromList [("name",String "default")],fromList [("name",String "system")]]
```
The 'result' variable is in type strict hashmap wrapped in Either.

We can fetch values by keys. For example:
```haskell
case result of
    Left _-> print "err"
    Right (v:values)-> print $ HM.lookup "name" v
```

which returns

```
String "DEMO"
```

Three datatypes of query result are in support; they are CSV, JSON, and Text which are all implemented in the Hackage library. Also support Multiple fetches. 
>>>>>>> docs(README.md): add some docs
