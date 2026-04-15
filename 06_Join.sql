

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS products;

drop dictionary dict_users;
drop dictionary dict_products;

CREATE TABLE users
(
    user_id     UInt64,
    country     LowCardinality(String),
    is_banned   UInt8,
    signup_date Date
)
ENGINE = MergeTree
ORDER BY user_id;

CREATE TABLE products
(
    product_id       UInt64,
    category         LowCardinality(String),
    price_cents      UInt32,
    is_discontinued  UInt8
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE events
(
    event_date    Date,
    event_time    DateTime,
    event_type    LowCardinality(String),
    user_id       UInt64,
    product_id    UInt64,
    revenue_cents UInt32,
    session_id    UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id);

--- FILL WITH DATA
INSERT INTO users
SELECT
    number + 1 AS user_id,
    ['US','DE','IN','BR','JP'][1 + (number % 5)] AS country,
    (number % 100 = 0) AS is_banned, -- ~1% banned
    toDate('2025-01-01') + (number % 120) AS signup_date
FROM numbers(20000000);

INSERT INTO products
SELECT
    number + 1 AS product_id,
    ['Books','Electronics','Clothes','Home','Grocery'][1 + (number % 5)] AS category,
    100 + (number % 5000) AS price_cents,
    (number % 200 = 0) AS is_discontinued  -- ~0.5% discontinued
FROM numbers(500000);

INSERT INTO events
SELECT
    toDate('2025-03-01') + (number % 60) AS event_date,
    toDateTime(event_date) + (number % 86400) AS event_time,
    ['view','add_to_cart','purchase'][1 + (number % 3)] AS event_type,
    1 + (cityHash64(number) % 200000) AS user_id,
    1 + (cityHash64(number * 17) % 50000) AS product_id,
    if(event_type = 'purchase', 100 + (number % 5000), 0) AS revenue_cents,
    generateUUIDv4() AS session_id
FROM numbers(50000000);

SELECT
    (SELECT count() FROM users)    AS users,
    (SELECT count() FROM products) AS products,
    (SELECT count() FROM events)   AS events;

-- -----------------------------------------------------------------------------
-- 3) Baseline JOIN query + EXPLAIN
-- -----------------------------------------------------------------------------
/*
Business question:
  Revenue by (country, category) for recent purchases, excluding:
    - banned users
    - discontinued products

EXPLAIN in 3 useful forms:
  - EXPLAIN QUERY TREE  : logical tree (what operations exist)
  - EXPLAIN PLAN        : optimized plan
  - EXPLAIN PIPELINE    : physical execution pipeline (how it executes in threads)

WHAT TO POINT OUT IN EXPLAIN PIPELINE
----------------------------------------------------------------------------
When you run EXPLAIN PIPELINE for the JOIN query, look for these concepts:

A) Multiple reads
   You are reading:
     - events  (big)
     - users   (dimension)
     - products(dimension)

   In pipeline output, you typically see multiple "ReadFromMergeTree" branches.
   One branch for events, and separate branches for users/products.

B) Join build vs join probe
   Join typically does:
     - Build hash table from right table(s) (users/products)
     - Probe it for each row from left table (events)

   In pipeline output, look for something like:
     - "JoinBuild", "CreatingJoin", "HashJoinBuild", or a Join step upstream
       of the big events stream.
     - "JoiningTransform" / "JoinTransform" (names vary)

   Teaching line:
     "JOIN adds a *build phase* that costs CPU+memory and repeats per query,
      unless you replace it with a cached structure like dictionary/join-engine."

C) Aggregation and sorting
   Your query groups and orders:
     - GROUP BY country, category => look for "AggregatingTransform"
     - ORDER BY revenue DESC      => look for "SortingTransform" or "MergeSorting"

D) Compare to dictionary version later
   In dictionary query pipeline, you should NOT see separate reads for users/products
   and you should NOT see Join transforms/build stages.

This is the comparison students should make:
  JOIN pipeline has extra branches for right tables + join build/probe.
  dictGet pipeline is mostly: read events -> evaluate dictGet -> aggregate -> sort.
*/

-- Tag the query text so we can find it easily in system.query_log later.


--Этапы join:
--- from left table + where before join
--- load right tables, build hash tables in memory
--- for every row in left table perform lookup in hash tables
--- applies additional filters after join operation 
--- aggregation
--- sorting

EXPLAIN PLAN
SELECT
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;

EXPLAIN syntax
SELECT
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;





EXPLAIN PIPELINE graph=1
--EXPLAIN PIPELINE
SELECT
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;



-- “Resize = redistribute workload across threads”
-- SimpleSquashingTransform - Combines small blocks into bigger ones - efficiency

--- check join speed
/*TAG:JOIN_HASH*/
select
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue desc
settings join_algorithm='hash';
--settings join_algorithm='parallel_hash'
--settings join_algorithm='partial_merge'
--settings join_algorithm='grace_hash'
--settings join_algorithm='full_sorting_merge';

/*TAG:JOIN_PARALLEL_HASH*/
select	
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue desc
--settings join_algorithm='hash'
settings join_algorithm='parallel_hash';
--settings join_algorithm='partial_merge'
--settings join_algorithm='grace_hash'
--settings join_algorithm='full_sorting_merge';

/*TAG:JOIN_PARTIAL_MERGE*/
select	
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue desc
--settings join_algorithm='hash'
--settings join_algorithm='parallel_hash'
settings join_algorithm='partial_merge';
--settings join_algorithm='grace_hash'
--settings join_algorithm='full_sorting_merge';

/*TAG:JOIN_GRACE_HASH*/
select
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue desc
--settings join_algorithm='hash'
--settings join_algorithm='parallel_hash'
--settings join_algorithm='partial_merge'
settings join_algorithm='grace_hash';
--settings join_algorithm='full_sorting_merge';

/*TAG:JOIN_FULL_SORT*/
select
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue desc
--settings join_algorithm='hash'
--settings join_algorithm='parallel_hash'
--settings join_algorithm='partial_merge'
--settings join_algorithm='grace_hash'
settings join_algorithm='full_sorting_merge';


SYSTEM FLUSH LOGS;
select
	query_duration_ms,
	event_time,
	query,
    (query_duration_ms / 1000) AS query_duration,
    formatReadableSize(memory_usage) AS memory_usage,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_data
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '/*TAG:JOIN_%'
  OR query LIKE '/*TAG:JOIN_HASH%'
ORDER BY query_duration DESC
LIMIT 10;








/*TAG:JOIN_SIMPLE baseline 1*/
-- Run it join
-- baseline 1
select
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;

/*TAG:JOIN_RIGHT baseline 1*/
-- Run it join
-- events right 1
select
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM users AS u
INNER JOIN events    AS e ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;


-- ADD SOME DATA



system flush logs;
select
	event_time,
	query,
    (query_duration_ms / 1000) AS query_duration,
    formatReadableSize(memory_usage) AS memory_usage,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_data
FROM system.query_log
--WHERE query LIKE 'TAG:JOIN%'
ORDER BY event_time DESC
LIMIT 5;


-- Run it join
-- baseline 2
select
    /*TAG:JOIN_SIMPLE_ADDED*/
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;

-- Run it join
-- events right 2
select
	/*TAG:JOIN_RIGHT_ADDED*/
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM users AS u
INNER JOIN events    AS e ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;


select
	event_time,
	query,
    (query_duration_ms / 1000) AS query_duration,
    formatReadableSize(memory_usage) AS memory_usage,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_data
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '-- Run it%'
ORDER BY event_time DESC
LIMIT 5;


--- прирост по времени выполнения / памяти





SET join_use_nulls = 0;
-- 0 — The empty cells are filled with the default value of the corresponding field type.
-- 1 — JOIN behaves the same way as in standard SQL. 
-- The type of the corresponding field is converted to Nullable, and empty cells are filled with NULL.
-- Enable join_use_nulls when working with data that may contain NULL values or when consistency with standard SQL behavior is required.
-- Use join_use_nulls=1 when performing complex analytical queries that rely on the presence of NULL values for accurate results.

-- -----------------------------------------------------------------------------
-- 4) JOIN correctness: LEFT JOIN null/default mode (join_use_nulls)
-- -----------------------------------------------------------------------------
INSERT INTO events VALUES
(
    toDate('2025-04-29'), now(), 'purchase',
    999999999,  -- missing user
    999999999,  -- missing product
    1234,
    generateUUIDv4()
);

SET join_use_nulls = 0;
-- Use nulls 0
SELECT
    e.user_id,
    u.country AS country_default_mode,
    p.category AS category_default_mode
FROM events e
LEFT JOIN users u    ON e.user_id = u.user_id
LEFT JOIN products p ON e.product_id = p.product_id
WHERE e.user_id = 999999999
LIMIT 1;

SET join_use_nulls = 1;
-- Use nulls 1
SELECT
    e.user_id,
    u.country AS country_null_mode,
    p.category AS category_null_mode
FROM events e
LEFT JOIN users u    ON e.user_id = u.user_id
LEFT JOIN products p ON e.product_id = p.product_id
WHERE e.user_id = 999999999
LIMIT 1;

SET join_use_nulls = 0;

select
	event_time,
	query,
    (query_duration_ms / 1000) AS query_duration,
    formatReadableSize(memory_usage) AS memory_usage,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_data
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '-- Use nulls%'
ORDER BY event_time DESC
LIMIT 5;

SET join_use_nulls = 0;
-- Use nulls 0 large
SELECT
    e.user_id,
    u.country AS country_default_mode,
    p.category AS category_default_mode
FROM events e
LEFT JOIN users u    ON e.user_id = u.user_id
LEFT JOIN products p ON e.product_id = p.product_id;

SET join_use_nulls = 1;
-- Use nulls 1 large
SELECT
    e.user_id,
    u.country AS country_null_mode,
    p.category AS category_null_mode
FROM events e
LEFT JOIN users u    ON e.user_id = u.user_id
LEFT JOIN products p ON e.product_id = p.product_id;

SET join_use_nulls = 0;

select
	event_time,
	query,
    (query_duration_ms / 1000) AS query_duration,
    formatReadableSize(memory_usage) AS memory_usage,
    formatReadableQuantity(read_rows) AS read_rows,
    formatReadableSize(read_bytes) AS read_data
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query LIKE '-- Use nulls%'
ORDER BY event_time DESC
LIMIT 5;

--- little memory overhead for nulls on
--- results may differ!
-- data corruption risk
-- left join + default value = missing user or real empty value
-- for null value = real missing user


-- -----------------------------------------------------------------------------
-- 5) JOIN correctness pitfall: dimension duplicates => fact multiplication
-- -----------------------------------------------------------------------------
/*
  If dimension is not unique on the join key, INNER JOIN multiplies rows.
  This is a *data quality / modeling* issue, but it shows up as:
    - wrong aggregates (too big)
    - slower queries (more rows after join)
*/

DROP TABLE IF EXISTS users_bad;
CREATE TABLE users_bad
(
    user_id UInt64,
    country LowCardinality(String),
    is_banned UInt8
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO users_bad SELECT user_id, country, is_banned FROM users;

-- Duplicate 1000 users
INSERT INTO users_bad
SELECT
    user_id,
    concat(country, '_DUP') AS country,
    is_banned
FROM users
WHERE user_id <= 1000;

SELECT
    count() AS purchases_join_users
FROM events e
INNER JOIN users u ON e.user_id = u.user_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30;
-- no any 7333326
-- any 200000

SELECT
    count() AS purchases_join_users_bad_multiplied
FROM events e
INNER JOIN users_bad u ON e.user_id = u.user_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30;
-- no any 7369982
-- any 200000

-- diff 36656

-- ANY JOIN prevents multiplication (but hides duplicates!)
SELECT
    count() AS purchases_any_join_users_bad
FROM events e
any INNER JOIN users_bad u ON e.user_id = u.user_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30;



-- -----------------------------------------------------------------------------
-- 6) Dictionaries: avoid JOIN build cost for dimension lookups
-- -----------------------------------------------------------------------------
/*
Dictionaries = server-managed lookup structures.

What to teach:
  - JOIN often rebuilds hash tables each query.
  - Dictionary is cached and reused across queries (until refreshed).
  - For dimension attributes (country, category, flags), dictGet is often ideal.

Critical caveat:
  - dictGet is a lookup per row. It’s fast, but still CPU work.
  - Works best when dimension is small/medium and fits in memory.
*/

DROP DICTIONARY IF EXISTS dict_users;
DROP DICTIONARY IF EXISTS dict_products;




CREATE DICTIONARY dict_users
(
    user_id UInt64,
    country String,
    is_banned UInt8
)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'username'
    PASSWORD 'password'
    DB 'learn_db'
    TABLE 'users'
))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());

CREATE DICTIONARY dict_products
(
    product_id UInt64,
    category String,
    price_cents UInt32,
    is_discontinued UInt8
)
PRIMARY KEY product_id
SOURCE(CLICKHOUSE(
    HOST 'localhost'
    PORT 9000
    USER 'username'
    PASSWORD 'password'
    DB 'learn_db'
    TABLE 'products'
))
LIFETIME(MIN 60 MAX 300)
LAYOUT(HASHED());

SELECT
    name,
    status,
    element_count,
    formatReadableSize(bytes_allocated),
    load_factor,
    last_exception
FROM system.dictionaries
--WHERE database = 'learn_db'
ORDER BY name;

select * from learn_db.dict_products;
select * from learn_db.dict_users;

-- dictionary loads after first user


-- -----------------------------------------------------------------------------
/*
WHAT TO COMPARE IN EXPLAIN PIPELINE vs JOIN:
  - JOIN query pipeline: separate reads for users/products + join build/probe stages.
  - dictGet query pipeline:
      *only reads events*
      then ExpressionTransform evaluates dictGet calls
      then aggregation/sort.

*/



-- recreate tables, refill data
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS products;

CREATE TABLE users
(
    user_id     UInt64,
    country     LowCardinality(String),
    is_banned   UInt8,
    signup_date Date
)
ENGINE = MergeTree
ORDER BY user_id;

CREATE TABLE products
(
    product_id       UInt64,
    category         LowCardinality(String),
    price_cents      UInt32,
    is_discontinued  UInt8
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE TABLE events
(
    event_date    Date,
    event_time    DateTime,
    event_type    LowCardinality(String),
    user_id       UInt64,
    product_id    UInt64,
    revenue_cents UInt32,
    session_id    UUID
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type, user_id);

INSERT INTO users
SELECT
    number + 1 AS user_id,
    ['US','DE','IN','BR','JP'][1 + (number % 5)] AS country,
    (number % 100 = 0) AS is_banned,                         -- ~1% banned
    toDate('2025-01-01') + (number % 120) AS signup_date
FROM numbers(2000000);

INSERT INTO products
SELECT
    number + 1 AS product_id,
    ['Books','Electronics','Clothes','Home','Grocery'][1 + (number % 5)] AS category,
    100 + (number % 5000) AS price_cents,
    (number % 200 = 0) AS is_discontinued                    -- ~0.5% discontinued
FROM numbers(500000);

INSERT INTO events
SELECT
    toDate('2025-03-01') + (number % 60) AS event_date,
    toDateTime(event_date) + (number % 86400) AS event_time,
    ['view','add_to_cart','purchase'][1 + (number % 3)] AS event_type,
    1 + (cityHash64(number) % 200000) AS user_id,
    1 + (cityHash64(number * 17) % 50000) AS product_id,
    if(event_type = 'purchase', 100 + (number % 5000), 0) AS revenue_cents,
    generateUUIDv4() AS session_id
FROM numbers(20000000);


EXPLAIN PLAN

SELECT
    dictGetOrDefault('learn_db.dict_users', 'country', e.user_id, 'UNKNOWN') AS country,
    dictGetOrDefault('learn_db.dict_products', 'category', e.product_id, 'UNKNOWN') AS category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND dictGetUInt8('learn_db.dict_users', 'is_banned', e.user_id) = 0
  AND dictGetUInt8('learn_db.dict_products', 'is_discontinued', e.product_id) = 0
GROUP BY country, category
ORDER BY revenue DESC;

EXPLAIN PIPELINE
SELECT
    dictGetOrDefault('learn_db.dict_users', 'country', e.user_id, 'UNKNOWN') AS country,
    dictGetOrDefault('learn_db.dict_products', 'category', e.product_id, 'UNKNOWN') AS category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND dictGetUInt8('learn_db.dict_users', 'is_banned', e.user_id) = 0
  AND dictGetUInt8('learn_db.dict_products', 'is_discontinued', e.product_id) = 0
GROUP BY country, category
ORDER BY revenue DESC;

-- Run it dict
select
	/*TAG:DICT_SIMPLE*/
    dictGetOrDefault('learn_db.dict_users', 'country', e.user_id, 'UNKNOWN') AS country,
    dictGetOrDefault('learn_db.dict_products', 'category', e.product_id, 'UNKNOWN') AS category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND dictGetUInt8('learn_db.dict_users', 'is_banned', e.user_id) = 0
  AND dictGetUInt8('learn_db.dict_products', 'is_discontinued', e.product_id) = 0
GROUP BY country, category
ORDER BY revenue DESC;

SYSTEM FLUSH LOGS;

-- Compare query_log of both tags side-by-side
SELECT
    if(query LIKE '%TAG:JOIN_BASELINE%', 'JOIN_BASELINE',
       if(query LIKE '%TAG:DICT_SIMPLE%', 'DICT_SIMPLE', 'OTHER')) AS tag,
    event_time,
    query_duration_ms,
    read_rows,
    read_bytes,
    memory_usage,
    ProfileEvents['JoinBuildTime'] AS join_build_time
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = 'learn_db'
  AND (query LIKE '%TAG:JOIN_BASELINE%' OR query LIKE '%TAG:DICT_SIMPLE%')
ORDER BY event_time DESC
LIMIT 20;



--- Очень полезная опция
--- без join операции
--- поиск записей, которые не имеют соответствия при JOIN двух таблиц
SELECT count() AS events_with_unknown_user
FROM events e
WHERE dictHas('learn_db.dict_users', e.user_id) = 0;


select 
	count()
from events e
anti join dict_users du on e.user_id = du.user_id;

select 
	count()
from events e
left join dict_users du on e.user_id = du.user_id
where du.user_id = 0;
 



/*AVOID_JOIN*/
SELECT
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
INNER JOIN users    AS u ON e.user_id    = u.user_id
INNER JOIN products AS p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;

/*AVOID_DICT_GET*/
SELECT
    dictGet('dict_users', 'country', e.user_id) AS country,
    dictGet('dict_products', 'category', e.product_id) AS category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events AS e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND dictGet('dict_users', 'is_banned', e.user_id) = 0
  AND dictGet('dict_products', 'is_discontinued', e.product_id) = 0
GROUP BY country, category
ORDER BY revenue DESC;


/*AVOID_DIRECT_JOIN*/
SELECT
    u.country,
    p.category,
    sum(e.revenue_cents) / 100.0 AS revenue
FROM events e
INNER JOIN dictionary('learn_db.dict_users')    u ON e.user_id = u.user_id
INNER JOIN dictionary('learn_db.dict_products') p ON e.product_id = p.product_id
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND u.is_banned = 0
  AND p.is_discontinued = 0
GROUP BY u.country, p.category
ORDER BY revenue DESC;

SYSTEM FLUSH LOGS;

select count() from events;
select
	event_time,
    query,
    query_duration_ms,
    formatReadableQuantity(read_rows),
    formatReadableSize(read_bytes),
    formatReadableSize(memory_usage),
    formatReadableQuantity(result_rows)
FROM system.query_log
WHERE query LIKE '/*AVOID_%'
	and query_duration_ms > 0
	and read_bytes > 0
ORDER BY event_time DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 7) Semi-join alternatives (IN / dictionary predicate)
-- -----------------------------------------------------------------------------
/*
When you only need to FILTER, not fetch columns, you can avoid JOIN.

Teaching point:
  - JOIN brings columns.
  - IN/EXISTS answers "is the key present?" (semi-join).
  - dictGet/dictHas can be an even faster semi-join for dimension flags.
*/

EXPLAIN PLAN
SELECT
    /*TAG:SEMIJOIN_NOT_IN*/
    count() AS purchases_not_in_banned
FROM events e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND e.user_id NOT IN (SELECT user_id FROM users WHERE is_banned = 1);

SELECT
    /*TAG:SEMIJOIN_NOT_IN*/
    count() AS purchases_not_in_banned
FROM events e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND e.user_id NOT IN (SELECT user_id FROM users WHERE is_banned = 1);

EXPLAIN PLAN
SELECT
    /*TAG:SEMIJOIN_DICT*/
    count() AS purchases_dict_not_banned
FROM events e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND dictGetUInt8('learn_db.dict_users', 'is_banned', e.user_id) = 0;

SELECT
    /*TAG:SEMIJOIN_DICT*/
    count() AS purchases_dict_not_banned
FROM events e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
  AND dictGetUInt8('learn_db.dict_users', 'is_banned', e.user_id) = 0;

-- -----------------------------------------------------------------------------
-- 8) Join Engine + joinGet (manual, very fast, you manage refresh)
-- -----------------------------------------------------------------------------
/*
Join engine stores a join structure as a table.
Compared to dictionary:
  - dictionary: server refreshes automatically based on LIFETIME
  - join engine: you load/refresh manually (INSERT/TRUNCATE/INSERT)

Compared to JOIN:
  - avoids per-query hash build
  - still gives lookup semantics (joinGet)
*/

DROP TABLE IF EXISTS users_join;

CREATE TABLE users_join
(
    user_id   UInt64,
    country   String,
    is_banned UInt8
)
ENGINE = Join(ANY, LEFT, user_id);

INSERT INTO users_join
SELECT user_id, country, is_banned FROM users;

SELECT
    /*TAG:JOIN_ENGINE*/
    joinGet('learn_db.users_join', 'country', e.user_id) AS country,
    count() AS purchases
FROM events e
WHERE e.event_type = 'purchase'
  AND e.event_date >= toDate('2025-04-29') - 30
GROUP BY country
ORDER BY purchases DESC
LIMIT 10;


 

