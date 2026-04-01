drop table join_table1;
CREATE TABLE join_table1
(
    id UInt64,
    value String
)
ENGINE = Log

drop table join_table2;
CREATE TABLE join_table2
(
    id UInt64,
    value String
)
ENGINE = Log


INSERT INTO join_table1 (*) VALUES (0, 'a'), (1, 'b'), (2, 'a'), (2, 'd'), (3, 'a')

INSERT INTO join_table2 (*) VALUES (1, 'e'), (2, 'f'), (3, 's'), (3, 'q'), (4, 'w')



select *
from join_table1
full join join_table2 using(id);


select *
from join_table1
inner join join_table2 using(id);
--- 0,4 отсутствуют
--- 2,3 появляются дважды - декартово произведение

select *
from join_table1
any inner join join_table2 using(id);
-- any отключит декартово произведение - повторов не будет
-- из нескольких одинаковых строк будет выбрана первая попвашая в результат


select *
from join_table1 t1
left join join_table2 t2 ON t1.id = t2.id;



select *
from join_table1 t1
any left join join_table2 t2 ON t1.id = t2.id;

-- any для left join работает идентично - убирает декартово произведение 
-- берет первую попавшуюся строку


select *
from join_table1 t1
full join join_table2 t2 ON t1.id = t2.id

-- left + right join 
--- any не применим


-- полное декартово произведение - M x N строк
-- тяжелая операция 
select *
from join_table1
cross join join_table2




---- специальные виды JOIN

drop table join_table1;
CREATE TABLE join_table1
(
    id UInt64,
    dt DateTime,
    val String
)
ENGINE = Log

drop table join_table2;
CREATE TABLE join_table2
(
    id UInt64,
    dt DateTime,
    val String
)
ENGINE = Log



INSERT INTO join_table1 (*) VALUES 
(0, '2020-01-01', 'a'), 
(1, '2020-01-02', 'b'), 
(2, '2020-01-03', 'a'), 
(3, '2020-01-02', 'd'), 
(3, '2020-01-04', 'a'), 
(4, '2020-01-03', 'a');

INSERT INTO join_table2 (*) VALUES 
(1, '2020-01-02', 'e'), 
(1, '2020-01-02', 'f'), 
(2, '2020-01-03', 's'), 
(3, '2020-01-04', 'q'), 
(4, '2020-01-05', 'w'), 
(5, '2020-01-07', 'p')



select id, val, join_table2.val
from join_table1
left JOIN join_table2 using(id)

--0	a	
--1	b	e
--1	b	f
--2	a	s
--3	d	q
--3	a	q
--4	a	w

select id, val, join_table2.val
from join_table1
SEMI JOIN join_table2 using(id)

--1	b	e
--2	a	s
--3	d	q
--3	a	q
--4	a	w


select id, val, join_table2.val
from join_table1
ANTI JOIN join_table2 using(id);

select id, val, join_table2.val
from join_table1
PASTE JOIN join_table2 using(id);



select *
from join_table1
LEFT JOIN join_table2 using(id, dt)
--0	2020-01-01 03:00:00	a	
--1	2020-01-02 03:00:00	b	e
--1	2020-01-02 03:00:00	b	f
--2	2020-01-03 03:00:00	a	s
--3	2020-01-02 03:00:00	d	
--3	2020-01-04 03:00:00	a	q
--4	2020-01-03 03:00:00	a	


select *
from join_table1
ASOF LEFT JOIN join_table2 using(id, dt)
--0	2020-01-01 03:00:00	a	
--1	2020-01-02 03:00:00	b	e
--2	2020-01-03 03:00:00	a	s
--3	2020-01-02 03:00:00	d	
--3	2020-01-04 03:00:00	a	q
--4	2020-01-03 03:00:00	a	


--- ASOF
drop table trades;
CREATE TABLE trades
(
    trade_id UInt32,
    symbol String,
    trade_time DateTime
)
ENGINE = MergeTree
ORDER BY (symbol, trade_time);

INSERT INTO trades VALUES
(1, 'AAPL', '2024-01-01 10:00:05'),
(2, 'AAPL', '2024-01-01 10:00:10'),
(3, 'AAPL', '2024-01-01 10:00:20');

drop table quotes;
CREATE TABLE quotes
(
    symbol String,
    quote_time DateTime,
    price Float64
)
ENGINE = MergeTree
ORDER BY (symbol, quote_time);

INSERT INTO quotes VALUES
('AAPL', '2024-01-01 10:00:00', 150),
('AAPL', '2024-01-01 10:00:07', 151),
('AAPL', '2024-01-01 10:00:15', 152);


SELECT
    t.trade_id,
    t.trade_time,
    q.quote_time,
    q.price
FROM trades t
ASOF JOIN quotes q
ON t.symbol = q.symbol
AND t.trade_time >= q.quote_time;


SELECT *
FROM trades t
JOIN quotes q
ON t.symbol = q.symbol
AND t.trade_time = q.quote_time;


















------ join algos




create table nyc_taxi.taxi_zone
(
  `LocationID` UInt16 DEFAULT 0,
  `Borough` String,
  `Zone` String,
  `service_zone` String
)
PRIMARY KEY LocationID
engine = TinyLog;

insert into nyc_taxi.taxi_zone
select 
  `LocationID`,
  `Borough`,
  `Zone`,
  `service_zone`
from HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv' FORMAT 'CSVWithNames');



CREATE DICTIONARY nyc_taxi.taxi_zone_dictionary
(
  `LocationID` UInt16 DEFAULT 0,
  `Borough` String,
  `Zone` String,
  `service_zone` String
)
PRIMARY KEY LocationID
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED_ARRAY())


drop table if exists trips;
CREATE TABLE trips (
    trip_id             UInt32,
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    pickup_longitude    Nullable(Float64),
    pickup_latitude     Nullable(Float64),
    dropoff_longitude   Nullable(Float64),
    dropoff_latitude    Nullable(Float64),
    passenger_count     UInt8,
    trip_distance       Float32,
    fare_amount         Float32,
    extra               Float32,
    tip_amount          Float32,
    tolls_amount        Float32,
    total_amount        Float32,
    payment_type        Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5),
    pickup_ntaname      LowCardinality(String),
    dropoff_ntaname     LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (pickup_datetime, dropoff_datetime);

drop table zones;
CREATE TABLE zones
(
    location_id UInt16,
    borough String,
    zone String
)
ENGINE = TinyLog;

INSERT INTO zones
SELECT *
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv',
    'CSVWithNames'
);



INSERT INTO trips
SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    tip_amount,
    tolls_amount,
    total_amount,
    payment_type,
    pickup_ntaname,
    dropoff_ntaname
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{0..2}.gz',
    'TabSeparatedWithNames'
);

SELECT count() FROM trips; -- 3kk+
SELECT count() FROM zones; -- 265


SELECT count()
FROM trips t
JOIN zones z
    ON t.pickup_ntaname = z.zone;



SELECT
    z.borough,
    count() AS trips_count
FROM trips t
INNER JOIN zones z
    ON lowerUTF8(t.pickup_ntaname) = lowerUTF8(z.zone)
GROUP BY z.borough
ORDER BY trips_count DESC;





SELECT
    z.borough,
    count()
FROM trips t
JOIN zones z
    ON lowerUTF8(t.pickup_ntaname) = lowerUTF8(z.zone)
GROUP BY z.borough
settings join_algorithm = 'hash';


SELECT
    z.borough,
    count()
FROM trips t
JOIN zones z
    ON lowerUTF8(t.pickup_ntaname) = lowerUTF8(z.zone)
GROUP BY z.borough
settings join_algorithm = 'partial_merge';







select 1;


--- dictionary
drop dictionary zones_dict;
CREATE DICTIONARY zones_dict
(
    zone String,
    borough String
)
PRIMARY KEY zone
SOURCE(CLICKHOUSE(
    TABLE 'zones'
))
LIFETIME(1)
LAYOUT(HASHED());

CREATE DICTIONARY zones_dict
(
    zone String,
    borough String
)
PRIMARY KEY zone
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'zones'
))
LAYOUT(HASHED())
LIFETIME(0);
-- PRIMARY KEY zone - ключ lookup
-- HASHED - хранится в памяти
-- LIFETIME 0 - всегда актуально


SELECT
    dictGet('zones_dict', 'borough', lowerUTF8(pickup_ntaname)) AS borough,
    count() AS trips_count,
    round(avg(fare_amount), 2) AS avg_fare
FROM learn_db.trips
GROUP BY borough
ORDER BY trips_count DESC;

SELECT currentDatabase();

SELECT count()
FROM learn_db.trips;


SHOW DICTIONARIES;


SELECT
    pickup_ntaname,
    dictGet('learn_db.zones_dict', 'borough', lowerUTF8(pickup_ntaname)) AS borough
FROM learn_db.trips
LIMIT 10;