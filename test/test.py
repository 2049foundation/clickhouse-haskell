from clickhouse_driver import Client

client = Client('localhost')

print(client.execute('SELECT toDecimal128(17,36) AS x, x / 7'))