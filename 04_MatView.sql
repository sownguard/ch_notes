drop table if exists learn_db.sales;
create table learn_db.sales (
	"date" Date,
	product_id UInt32,
	quantity UInt32,
	price Decimal(10,2),
	total_amount Decimal(10,2) MATERIALIZED quantity * price)
engine = MergeTree()
order by "date";


drop table if exists learn_db.sales_daily_stats;
create table learn_db.sales_daily_stats (
	"date" Date,
	total_quantity AggregateFunction(sum, UInt32),
	total_revenue AggregateFunction(sum, Decimal(10,2)),
	avg_price AggregateFunction(avg, Decimal(10,2))
	)
engine = AggregatingMergeTree()
order by "date";

drop table if exists learn_db.sales_daily_stats_mv;
create materialized view learn_db.sales_daily_stats_mv to learn_db.sales_daily_stats as
select 
	"date",
	sumState(quantity) as total_quantity,
	sumState(total_amount) as total_revenue,
	avgState(price) as avg_price
from learn_db.sales
group by "date";

insert into learn_db.sales
select 
	date_add(DAY, rand() % 366, today() - INTERVAL 1 YEAR) AS "date",
	round(randUniform(1, 1000000)) AS product_id,
	round(randUniform(1, 1000)) AS quantity,
	toDecimal64(randUniform(1, 100000), 2) as price
from numbers(10000);

select count(*) from learn_db.sales_daily_stats;
select * from learn_db.sales;
select 
	"date",
	sumMerge(total_quantity),
	sumMerge(total_revenue),
	toDecimal64(avgMerge(avg_price),2)
from learn_db.sales_daily_stats 
group by "date";


select
	"date",
	sumMerge(total_quantity) as total_quantity,
	sumMerge(total_revenue) as total_amount,
	toDecimal64(avgMerge(avg_price),2) as avg_price
from learn_db.sales_daily_stats
where "date"='2025-04-03'
group by "date";

select
	"date",
	sum(quantity) as total_quantity,
	sum(total_amount) as total_amount,
	toDecimal64(avg(price),2) as avg_price
from learn_db.sales
where "date"='2025-04-03'
group by "date";








------ INCREMENTAL MV -------


drop table votes;
drop view up_down_votes_per_day_mv;
drop table up_down_votes_per_day;
drop view up_down_votes_agg_mv;
drop table up_down_votes_agg;


-- Create base table for raw data
CREATE TABLE votes
(
    `Id` UInt32,
    `PostId` Int32,
    `VoteTypeId` UInt8,
    `CreationDate` DateTime64(3, 'UTC'),
    `UserId` Int32,
    `BountyAmount` UInt8
)
ENGINE = MergeTree
ORDER BY (VoteTypeId, CreationDate, PostId)

--- insert data from ext source
INSERT INTO 
    votes 
SELECT * 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes/*.parquet')
limit 1_000_000;


select count() from votes;
--- check table
--SELECT 
--    toStartOfDay(CreationDate) AS day,
--    countIf(VoteTypeId = 2) AS UpVotes,
--    countIf(VoteTypeId = 3) AS DownVotes
--FROM votes
--GROUP BY day
--ORDER BY day ASC
--LIMIT 10;



--- Create table for count per day
CREATE TABLE up_down_votes_per_day
(
  `Day` Date,
  `UpVotes` UInt32,
  `DownVotes` UInt32
)
ENGINE = SummingMergeTree
ORDER BY Day;

-- table empty
select count() from up_down_votes_per_day;



--- Create matview
CREATE MATERIALIZED VIEW up_down_votes_per_day_mv 
	TO up_down_votes_per_day 
	AS
		SELECT 
		   toStartOfDay(CreationDate)::Date AS Day,
		   countIf(VoteTypeId = 2) AS UpVotes,
		   countIf(VoteTypeId = 3) AS DownVotes
		FROM votes
		GROUP BY Day;

select * from up_down_votes_per_day_mv;
select 
	* 
from system.tables 
where name like 'up_down_votes_%';

--- add data from ext source
INSERT INTO 
    votes 
SELECT * 
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes/*.parquet')
limit 100_000
offset 1_000_000
;

select * from up_down_votes_per_day_mv;
select * from up_down_votes_per_day;




select * from system.tables where name like 'up_down_votes%';




--- Manual skip unmerged unmerged parts
--- with FINAL
SELECT
        Day,
        UpVotes,
        DownVotes
FROM up_down_votes_per_day
FINAL
ORDER BY Day ASC
LIMIT 10;


--- with order / sum
SELECT 
    Day, 
    sum(UpVotes) AS UpVotes, 
    sum(DownVotes) AS DownVotes
FROM up_down_votes_per_day
GROUP BY Day
ORDER BY Day ASC
LIMIT 10;




-- JOIN DEMO
create table mv_test (
 x Int8)
engine = MergeTree()
order by x;


create table mv_test_counter (
	x_cnt Int8)
engine = MergeTree()
order by x_cnt;

create materialized view mv_test_mv to mv_test_counter
as 	
	select 
		count(x) as x_cnt
	from mv_test;

select * from mv_test;
select * from mv_test_counter;


insert into mv_test
values (0);

select * from mv_test;
-- 1
select * from mv_test_counter;
-- 1

insert into mv_test
values (1),(2),(3);
select * from mv_test;
-- 0, 1, 2, 3
select * from mv_test_counter;
-- 1, 3

insert into mv_test
values (4);

select * from mv_test;
-- 0, 1, 2, 3, 4

select * from mv_test_counter;
-- 1, 3, 1

truncate table mv_test;
truncate table mv_test_counter;


--- join extended

CREATE TABLE badges
(
    `Id` UInt32,
    `UserId` Int32,
    `Name` LowCardinality(String),
    `Date` DateTime64(3, 'UTC'),
    `Class` Enum8('Gold' = 1, 'Silver' = 2, 'Bronze' = 3),
    `TagBased` Bool
)
ENGINE = MergeTree
ORDER BY UserId

CREATE TABLE users
(
    `Id` Int32,
    `Reputation` UInt32,
    `CreationDate` DateTime64(3, 'UTC'),
    `DisplayName` LowCardinality(String),
    `LastAccessDate` DateTime64(3, 'UTC'),
    `Location` LowCardinality(String),
    `Views` UInt32,
    `UpVotes` UInt32,
    `DownVotes` UInt32
)
ENGINE = MergeTree
ORDER BY Id;

truncate table users;

--- already inserted
INSERT INTO users
SELECT * FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet')
limit 100_000;

select * from users
limit 10;
-- Rosemary
-- konstantin314
-- strohs

CREATE TABLE daily_badges_by_user
(
    Day Date,
    UserId Int32,
    DisplayName LowCardinality(String),
    Gold UInt32,
    Silver UInt32,
    Bronze UInt32
)
ENGINE = SummingMergeTree
ORDER BY (DisplayName, UserId, Day);

--drop view daily_badges_by_user_mv;

CREATE MATERIALIZED VIEW daily_badges_by_user_mv TO daily_badges_by_user AS
SELECT
    toDate(Date) AS Day,
    b.UserId,
    u.DisplayName,
    countIf(Class = 'Gold') AS Gold,
    countIf(Class = 'Silver') AS Silver,
    countIf(Class = 'Bronze') AS Bronze
FROM badges AS b
LEFT JOIN users AS u ON b.UserId = u.Id
GROUP BY Day, b.UserId, u.DisplayName;

truncate table badges;
truncate table daily_badges_by_user;

INSERT INTO badges SELECT *
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/badges.parquet')
limit 100_000;

select count() from badges;
select count() from daily_badges_by_user;

select distinct(DisplayName) from daily_badges_by_user;

--- 0.124s
INSERT INTO badges VALUES (53505058, 2936484, 'gingerwizard', now(), 'Gold', 0);


--- filter for join
CREATE MATERIALIZED VIEW daily_badges_by_user_mv TO daily_badges_by_user
AS SELECT
    toDate(Date) AS Day,
    b.UserId,
    u.DisplayName,
    countIf(Class = 'Gold') AS Gold,
    countIf(Class = 'Silver') AS Silver,
    countIf(Class = 'Bronze') AS Bronze
FROM badges AS b
LEFT JOIN
(
    SELECT
        Id,
        DisplayName
    FROM users
    WHERE Id IN (
        SELECT UserId
        FROM badges
    )
) AS u ON b.UserId = u.Id
GROUP BY
    Day,
    b.UserId,
    u.DisplayName
    

--- aggMR example

drop table up_down_votes_agg;

CREATE TABLE up_down_votes_agg(	
  CreationDate DateTime64(3, 'UTC'),
  Id AggregateFunction(uniq, UInt32),
  PostId AggregateFunction(uniq, Int32),
  UserId AggregateFunction(uniq, Int32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (CreationDate);


insert into up_down_votes_agg
select 
	CreationDate,
	uniqState(Id) as Id,
	uniqState(PostId) as PostId,
	uniqState(UserId) as UserId
from votes
group by CreationDate;

truncate up_down_votes_agg;
select 
	CreationDate,
	uniqMerge(Id) as uid,
	uniqMerge(PostId) as upid,
	uniqMerge(UserId) as uuid
from up_down_votes_agg
group by CreationDate
order by upid desc;
--order by uniqMerge(PostId);

CREATE MATERIALIZED VIEW up_down_votes_agg_mv
to up_down_votes_agg
as 
select 
	CreationDate,
	uniqState(Id) as Id,
	uniqState(PostId) as PostId,
	uniqState(UserId) as UserId
from votes
group by CreationDate;




drop table learn_db.orders;
CREATE TABLE learn_db.orders (
    order_id UInt32,
    user_id UInt32,
    product_id UInt32,
    amount Decimal(18, 2),
    order_date Date
) ENGINE = MergeTree()
ORDER BY (product_id, order_date);

DROP TABLE IF EXISTS learn_db.orders_agg;
CREATE TABLE learn_db.orders_agg (
    order_date Date,
    orders AggregateFunction(uniq, UInt32),
    products AggregateFunction(uniq, UInt32),
    users AggregateFunction(uniq, UInt32)   
) 
ENGINE = AggregatingMergeTree()
ORDER BY (order_date);

create materialized view learn_db.orders_uniq_mv
	to learn_db.orders_agg
	as
		select 
			order_date,
			uniqState(order_id) as orders,
			uniqState(product_id) as products,
			uniqState(order_id) as users
		from learn_db.orders
		group by order_date;
			
select * from learn_db.orders_uniq_mv;
select 
	order_date, 
	uniqMerge(users) 
from learn_db.orders_agg 
group by order_date;

insert into learn_db.orders
(order_id, user_id, product_id, amount, order_date)
values
(1, 1, 1, 10, '2025-01-01'),
(2, 2, 1, 10, '2025-01-01'),
(3, 1, 2, 5, '2025-01-01'),
(4, 2, 2, 5, '2025-01-01');




--- 
--- Refreshable mat views
---

CREATE MATERIALIZED VIEW table_name_mv
REFRESH EVERY 1 MINUTE TO table_name AS
...


--- Force refresh
SYSTEM REFRESH VIEW table_name_mv;
SYSTEM STOP/START view  <view_name>;


--- Info about view runs
SELECT database, view, status,
       last_success_time, last_refresh_time, next_refresh_time,
       read_rows, written_rows
FROM system.view_refreshes
where name = 'view_name';


-- Change refresh timer
ALTER TABLE table_name_mv
MODIFY REFRESH EVERY 30 SECONDS;


drop table rmv_events;

create table rmv_events (
	ts DateTime64,
	uuid UUID,
	amount UInt8)
engine = MergeTree()
order by ts;

insert into rmv_events
select 
	now(),
	generateUUIDv4(),
	randUniform(0, 256)
from numbers(100);

select * from rmv_events;

select 
	uuid, 
	count() as cnt
from rmv_events
group by uuid;