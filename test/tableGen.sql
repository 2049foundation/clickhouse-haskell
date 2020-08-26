CREATE TABLE IF NOT EXISTS crd3 (
    `id` Nullable(FixedString(3)),
    `card` LowCardinality(String),
    `cardn` LowCardinality(Nullable(String))
) 
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO crd3 VALUES ('abc', 'myString', null), ('xyz', 'Noctis', 'Ross'), ('123', 'Alice', null), ('456','Bob','Walter')