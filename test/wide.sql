CREATE TABLE IF NOT EXISTS wide (
    `id` Nullable(FixedString(3)),
    `date` Date,
    `ipv4` IPv4,
    `ipv6` IPv6,
) 
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO wide VALUES ('abc', '2020-07-07', '127.0.0.0', '2001:0db8:85a3:0000:0000:8a2e:0370:7334')