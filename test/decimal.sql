CREATE TABLE IF NOT EXISTS decimals (
    `number` Decimal32(2),
    `number2` Decimal64(5),
    `number3` Decimal(10, 5)
) 
ENGINE = MergeTree()
ORDER BY tuple();


