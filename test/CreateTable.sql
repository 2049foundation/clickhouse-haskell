CREATE TABLE IF NOT EXISTS default.test_table (
    `id` FixedString(10),
    `item` String,
    `number` Int16,
    `numArray` Array(Int16)
)
ENGINE = Memory()
PRIMARY KEY id;

INSERT INTO default.test_table (`id`, `item`,`number`,`numArray`) VALUES ('0000000001', 'JOHN',1557,[45,45,45]),('1234567890', 'CONNOR',533,[1,2,3,4]),('3543364534', 'MARRY',220,[0,1,2,3,121,2]),('2258864346', 'JAME',4452,[42,54548,66,676,0])
