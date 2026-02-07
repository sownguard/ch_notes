-- drop table 
DROP TABLE learn_db.mart_carsharing_trip;

--
CREATE TABLE learn_db.mart_carsharing_trip
(
    trip_id UInt32,
    user_id UInt32 CODEC(Delta, ZSTD),
    user_guid String,
    city_id UInt16,
    car_id UInt32 CODEC(Delta, ZSTD),
    tariff_id UInt16,
    tariff_name String,
    trip_start_date Date,
    trip_start_month String,
    trip_start_year UInt16,
    trip_duration_min UInt16,
    trip_distance_km Float32,
    trip_cost Float32,
    load_date Date
)
ENGINE = MergeTree()
PRIMARY KEY (trip_start_date, user_id, trip_id)
AS
SELECT
    number + 1 AS trip_id,
    floor(randUniform(1, 500000)) AS user_id,
    toString(user_id) AS user_guid,
    user_id % 50 AS city_id,
    floor(randUniform(1, 20000)) AS car_id,
    car_id % 5 AS tariff_id,
    CASE tariff_id
        WHEN 0 THEN 'Minute'
        WHEN 1 THEN 'Hourly'
        WHEN 2 THEN 'Daily'
        WHEN 3 THEN 'Fixed'
        ELSE 'Premium'
    END AS tariff_name,
    cast(now() - randUniform(0, 3600 * 24 * 365) AS Date) AS trip_start_date,
    formatDateTime(trip_start_date, '%Y-%m') AS trip_start_month,
    toYear(trip_start_date) AS trip_start_year,
    floor(randUniform(3, 180)) AS trip_duration_min,
    round(randUniform(1, 120), 2) AS trip_distance_km,
    round(trip_duration_min *
        CASE tariff_id
            WHEN 0 THEN 7
            WHEN 1 THEN 350
            WHEN 2 THEN 2500
            WHEN 3 THEN 1200
            ELSE 15
        END, 2) AS trip_cost,

    trip_start_date + rand() % 3 AS load_date
FROM numbers(10);

-- view parts
SELECT * 
FROM system.parts 
WHERE table = 'mart_carsharing_trip';

SELECT * 
FROM system.part_log 
WHERE table = 'mart_carsharing_trip' 
ORDER BY event_time DESC;


-- add more rows
INSERT INTO learn_db.mart_carsharing_trip
WITH
    cast(now() - randUniform(0, 3600 * 24 * 365) AS Date) AS trip_start_date,
    floor(randUniform(1, 500000)) AS user_id,
    floor(randUniform(1, 20000)) AS car_id,
    car_id % 5 AS tariff_id,
    floor(randUniform(3, 180)) AS trip_duration_min
SELECT
    number + rand() AS trip_id,
    user_id,
    toString(user_id) AS user_guid,
    user_id % 50 AS city_id,
    car_id,
    tariff_id,
    CASE tariff_id
        WHEN 0 THEN 'Minute'
        WHEN 1 THEN 'Hourly'
        WHEN 2 THEN 'Daily'
        WHEN 3 THEN 'Fixed'
        ELSE 'Premium'
    END AS tariff_name,
    trip_start_date,
    formatDateTime(trip_start_date, '%Y-%m') AS trip_start_month,
    toYear(trip_start_date) AS trip_start_year,
    trip_duration_min,
    round(randUniform(1, 120), 2) AS trip_distance_km,
    round(trip_duration_min * (tariff_id + 5), 2) AS trip_cost,
    trip_start_date + rand() % 3 AS load_date
FROM numbers(10);

-- bash - benchmarking
echo "INSERT INTO learn_db.mart_carsharing_trip
SELECT
    number + rand(),
    floor(randUniform(1, 500000)) AS user_id,
    toString(user_id),
    user_id % 50 AS city_id,
    floor(randUniform(1, 20000)) AS car_id,
    car_id % 5,
    'Minute',
    today() - rand() % 365 AS trip_start_date,
    formatDateTime(trip_start_date, '%Y-%m'),
    toYear(trip_start_date),
    rand() % 180,
    rand() % 120,
    rand() % 5000,
    today()
FROM numbers(10);" > query.sql

clickhouse-benchmark -i 10000 -c 10 --query "`cat query.sql`"


-- view parts
SELECT * 
FROM system.parts 
WHERE table = 'mart_carsharing_trip';

SELECT * 
FROM system.part_log 
WHERE table = 'mart_carsharing_trip' 
ORDER BY event_time DESC;


-- force merge parts
OPTIMIZE TABLE learn_db.mart_carsharing_trip FINAL;

SELECT * 
FROM system.parts 
WHERE table = 'mart_carsharing_trip'
AND active;


--- show wide / compact storage type
INSERT INTO learn_db.mart_carsharing_trip
WITH
    cast(now() - randUniform(0, 3600 * 24 * 365) AS Date) AS trip_start_date,
    floor(randUniform(1, 500000)) AS user_id,
    floor(randUniform(1, 20000)) AS car_id,
    car_id % 5 AS tariff_id,
    floor(randUniform(3, 180)) AS trip_duration_min
SELECT
    number + rand() AS trip_id,
    user_id,
    toString(user_id) AS user_guid,
    user_id % 50 AS city_id,
    car_id,
    tariff_id,
    CASE tariff_id
        WHEN 0 THEN 'Minute'
        WHEN 1 THEN 'Hourly'
        WHEN 2 THEN 'Daily'
        WHEN 3 THEN 'Fixed'
        ELSE 'Premium'
    END AS tariff_name,
    trip_start_date,
    formatDateTime(trip_start_date, '%Y-%m') AS trip_start_month,
    toYear(trip_start_date) AS trip_start_year,
    trip_duration_min,
    round(randUniform(1, 120), 2) AS trip_distance_km,
    round(trip_duration_min * (tariff_id + 5), 2) AS trip_cost,
    trip_start_date + rand() % 3 AS load_date
FROM numbers(1000000);

SELECT * 
FROM system.parts 
WHERE table = 'mart_carsharing_trip'
AND active;


-- try async load
CREATE TABLE learn_db.async_carsharing
(
    id UInt32,
    ts DateTime,
    payload String
)
ENGINE = MergeTree
ORDER BY (id, ts);

SET async_insert = 1;


-- bash async benc load
echo "INSERT INTO learn_db.async_carsharing 
SETTINGS async_insert=1 
VALUES (1, now(), 'trip');" > async_query.sql
clickhouse-benchmark -i 1000000 -c 10 --query "`cat async_query.sql`"


SELECT * 
FROM system.parts 
WHERE table = 'async_test'
AND active;


SELECT * 
FROM system.part_log 
WHERE table = 'mart_carsharing_trip' 
ORDER BY event_time DESC;



--- base explain indexes
SELECT count() FROM learn_db.mart_carsharing_trip
WHERE user_id = 123;

explain indexes=1
SELECT count() FROM learn_db.mart_carsharing_trip
WHERE car_id = 123;

explain indexes=1
SELECT count() FROM learn_db.mart_carsharing_trip
WHERE trip_id > 1234;

explain indexes=1
SELECT count() FROM learn_db.mart_carsharing_trip
WHERE trip_start_date = toDate('2025-12-11');





--- partitions


CREATE TABLE learn_db.mart_carsharing_trip_parted
(
    trip_id UInt32,
    user_id UInt32 CODEC(Delta, ZSTD),
    user_guid String,
    city_id UInt16,
    car_id UInt32 CODEC(Delta, ZSTD),
    tariff_id UInt16,
    tariff_name String,
    trip_start_date Date,
    trip_start_month String,
    trip_start_year UInt16,
    trip_duration_min UInt16,
    trip_distance_km Float32,
    trip_cost Float32,
    load_date Date
)
ENGINE = MergeTree()
PRIMARY KEY (trip_start_date, user_id, trip_id)
PARTITION BY trip_start_month
AS
SELECT
    number + 1 AS trip_id,
    floor(randUniform(1, 500000)) AS user_id,
    toString(user_id) AS user_guid,
    user_id % 50 AS city_id,
    floor(randUniform(1, 20000)) AS car_id,
    car_id % 5 AS tariff_id,
    CASE tariff_id
        WHEN 0 THEN 'Minute'
        WHEN 1 THEN 'Hourly'
        WHEN 2 THEN 'Daily'
        WHEN 3 THEN 'Fixed'
        ELSE 'Premium'
    END AS tariff_name,
    cast(now() - randUniform(0, 3600 * 24 * 365) AS Date) AS trip_start_date,
    formatDateTime(trip_start_date, '%Y-%m') AS trip_start_month,
    toYear(trip_start_date) AS trip_start_year,
    floor(randUniform(3, 180)) AS trip_duration_min,
    round(randUniform(1, 120), 2) AS trip_distance_km,
    round(trip_duration_min *
        CASE tariff_id
            WHEN 0 THEN 7
            WHEN 1 THEN 350
            WHEN 2 THEN 2500
            WHEN 3 THEN 1200
            ELSE 15
        END, 2) AS trip_cost,
    trip_start_date + rand() % 3 AS load_date
FROM numbers(10000);

INSERT INTO learn_db.mart_carsharing_trip_parted
WITH
    cast(now() - randUniform(0, 3600 * 24 * 365) AS Date) AS trip_start_date,
    floor(randUniform(1, 500000)) AS user_id,
    floor(randUniform(1, 20000)) AS car_id,
    car_id % 5 AS tariff_id,
    floor(randUniform(3, 180)) AS trip_duration_min
SELECT
    number + rand() AS trip_id,
    user_id,
    toString(user_id) AS user_guid,
    user_id % 50 AS city_id,
    car_id,
    tariff_id,
    CASE tariff_id
        WHEN 0 THEN 'Minute'
        WHEN 1 THEN 'Hourly'
        WHEN 2 THEN 'Daily'
        WHEN 3 THEN 'Fixed'
        ELSE 'Premium'
    END AS tariff_name,
    trip_start_date,
    formatDateTime(trip_start_date, '%Y-%m') AS trip_start_month,
    toYear(trip_start_date) AS trip_start_year,
    trip_duration_min,
    round(randUniform(1, 120), 2) AS trip_distance_km,
    round(trip_duration_min * (tariff_id + 5), 2) AS trip_cost,
    trip_start_date + rand() % 3 AS load_date
FROM numbers(5000000);


SELECT * FROM system.parts
WHERE table = 'mart_carsharing_trip_parted'
and active;

SELECT * FROM system.merges;




---- partitions 

DROP TABLE IF EXISTS learn_db.orders;
CREATE TABLE learn_db.orders (
	order_id UInt32,
	user_id UInt32,
	product_id UInt32,
	amount Decimal(18,2),
	order_date Date
)
ENGINE = MergeTree()
ORDER BY (product_id);

INSERT INTO learn_db.orders 
SELECT 
	number AS order_id
	,round(randUniform(1, 100000)) AS user_id
	,round(randUniform(1, 1000000)) AS product_id
	,randUniform(1, 10000) AS amount
	,date_add(DAY, rand() % (365 * 10 + 2), today() - INTERVAL 10 YEAR)
FROM
	numbers(10000000);

SELECT DISTINCT _partition_id, _part 
FROM learn_db.orders 
ORDER BY _partition_id, _part;

DROP TABLE IF EXISTS learn_db.orders_parted_by_year;
CREATE TABLE learn_db.orders_parted_by_year (
	order_id UInt32,
	user_id UInt32,
	product_id UInt32,
	amount Decimal(18,2),
	order_date Date
)
ENGINE = MergeTree()
PARTITION BY toYear(order_date)
ORDER BY (product_id);

INSERT INTO learn_db.orders_parted_by_year
SELECT * FROM learn_db.orders;

SELECT DISTINCT _partition_id, _part 
FROM learn_db.orders_parted_by_year 
ORDER BY _partition_id, _part;

SELECT * FROM system.parts
WHERE table = 'orders_parted_by_year'
and active;
