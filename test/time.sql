CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;

INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 5);

