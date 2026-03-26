
-- LIGHTWEIGTH DELETES DEMO
drop table learn_db.lwdemo;
CREATE TABLE learn_db.lwdemo
(
    id UInt64,
    user_id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
-- forced wide part

INSERT INTO learn_db.lwdemo
SELECT
    number AS id,
    number % 5 AS user_id,
    toString(number)
FROM numbers(20);


select * from learn_db.lwdemo

SELECT
    database,
    table,
    data_paths
FROM system.tables
WHERE name = 'lwdemo';

select *, from system.parts
where table = 'lwdemo';




--- cd /var/lib/clickhouse/store/<hash>/<hash>/
--- ls -l

--- cd <partname>
--- ls -li

--- some .bin files
--- первая колонка - inode
select *, _row_exists from learn_db.lwdemo;
DELETE FROM learn_db.lwdemo WHERE user_id = 2;

-- show new column
select *, _row_exists from learn_db.lwdemo;

-- check new parts
select * from system.parts
where table = 'lwdemo';

SELECT *
FROM system.mutations
WHERE table = 'lwdemo';


-- wait for is_done = 1

-- check new part created
-- cd /var/lib/clickhouse/store/<hash>/<hash>/
-- ls -l

-- check inodes
-- ls -li <old part>
-- ls -li <new part>

-- new part has same inodes to column files
-- and new file _row_exists.bin

-- stat <old_part>/id.bin
-- stat <new_part>/id.bin

-- we see same inode nad num of links


-- check if data deleted
SELECT * FROM learn_db.lwdemo WHERE user_id = 2;
-- 0 rows

-- but data still in bin files  

-- perform force merge
OPTIMIZE TABLE learn_db.lwdemo FINAL;

-- check  parts
select * from system.parts
where table = 'lwdemo';

-- ls -l
-- we see new part

-- ls -li -> see new inodes - differs











----- DELETE MUTATIONS

DROP TABLE learn_db.mart_student_lesson;
CREATE TABLE learn_db.mart_student_lesson
(
	`student_profile_id` Int32, -- Идентификатор профиля обучающегося
	`person_id` String, -- GUID обучающегося
	`person_id_int` Int32 CODEC(Delta, ZSTD),
	`educational_organization_id` Int16, -- Идентификатор образовательной организации
	`parallel_id` Int16,
	`class_id` Int16, -- Идентификатор класса
	`lesson_date` Date32, -- Дата урока
	`lesson_month_digits` String,
	`lesson_month_text` String,
	`lesson_year` UInt16,
	`load_date` Date, -- Дата загрузки данных
	`t` Int16 CODEC(Delta, ZSTD),
	`teacher_id` Int32 CODEC(Delta, ZSTD), -- Идентификатор учителя
	`subject_id` Int16 CODEC(Delta, ZSTD), -- Идентификатор предмета
	`subject_name` String,
	`mark` Int8, -- Оценка
	PRIMARY KEY(lesson_date, person_id_int, mark)
) ENGINE = MergeTree()
PARTITION BY (educational_organization_id)
AS SELECT
	floor(randUniform(2, 1300000)) as student_profile_id,
	cast(student_profile_id as String) as person_id,
	cast(person_id as Int32) as  person_id_int,
    student_profile_id / 365000 as educational_organization_id,
    student_profile_id / 73000 as parallel_id,
    student_profile_id / 2000 as class_id,
    cast(now() - randUniform(2, 60*60*24*365) as date) as lesson_date, -- Дата урока
    formatDateTime(lesson_date, '%Y-%m') as lesson_month_digits,
    formatDateTime(lesson_date, '%Y %M') AS lesson_month_text,
    toYear(lesson_date) as lesson_year, 
    lesson_date + rand() % 3, -- Дата загрузки данных
    floor(randUniform(2, 137)) as t,
    educational_organization_id * 136 + t as teacher_id,
    floor(t/9) as subject_id,
    CASE subject_id
    	WHEN 1 THEN 'Математика'
    	WHEN 2 THEN 'Русский язык'
    	WHEN 3 THEN 'Литература'
    	WHEN 4 THEN 'Физика'
    	WHEN 5 THEN 'Химия'
    	WHEN 6 THEN 'География'
    	WHEN 7 THEN 'Биология'
    	WHEN 8 THEN 'Физическая культура'
    	ELSE 'Информатика'
    END as subject_name,
    CASE 
    	WHEN randUniform(0, 2) > 1
    		THEN -1
    		ELSE 
    			CASE
	    			WHEN ROUND(randUniform(0, 5)) + subject_id < 5 THEN ROUND(randUniform(4, 5))
	    			WHEN ROUND(randUniform(0, 5)) + subject_id < 9 THEN ROUND(randUniform(3, 5))
	    			ELSE ROUND(randUniform(2, 5))
    			END				
    END AS mark
FROM numbers(100000000);

-- check uniq org_ids
SELECT DISTINCT educational_organization_id 
FROM learn_db.mart_student_lesson 
ORDER BY educational_organization_id;

select * from system.parts
where 1=1
	and table = 'mart_student_lesson'
	and active;


-- FAST command+enter
ALTER TABLE learn_db.mart_student_lesson 
	DELETE WHERE educational_organization_id = 2;
-- we can see that rows with org_id = 2 exists in some moment
SELECT DISTINCT educational_organization_id 
FROM learn_db.mart_student_lesson 
ORDER BY educational_organization_id;

-- show no parts for org_id = 2
select * from system.parts
where 1=1
	and table = 'mart_student_lesson'
	and active;



-- recreate table for sync mutation

DROP TABLE learn_db.mart_student_lesson;
CREATE TABLE learn_db.mart_student_lesson
(
	`student_profile_id` Int32, -- Идентификатор профиля обучающегося
	`person_id` String, -- GUID обучающегося
	`person_id_int` Int32 CODEC(Delta, ZSTD),
	`educational_organization_id` Int16, -- Идентификатор образовательной организации
	`parallel_id` Int16,
	`class_id` Int16, -- Идентификатор класса
	`lesson_date` Date32, -- Дата урока
	`lesson_month_digits` String,
	`lesson_month_text` String,
	`lesson_year` UInt16,
	`load_date` Date, -- Дата загрузки данных
	`t` Int16 CODEC(Delta, ZSTD),
	`teacher_id` Int32 CODEC(Delta, ZSTD), -- Идентификатор учителя
	`subject_id` Int16 CODEC(Delta, ZSTD), -- Идентификатор предмета
	`subject_name` String,
	`mark` Int8, -- Оценка
	PRIMARY KEY(lesson_date, person_id_int, mark)
) ENGINE = MergeTree()
PARTITION BY (educational_organization_id)
AS SELECT
	floor(randUniform(2, 1300000)) as student_profile_id,
	cast(student_profile_id as String) as person_id,
	cast(person_id as Int32) as  person_id_int,
    student_profile_id / 365000 as educational_organization_id,
    student_profile_id / 73000 as parallel_id,
    student_profile_id / 2000 as class_id,
    cast(now() - randUniform(2, 60*60*24*365) as date) as lesson_date, -- Дата урока
    formatDateTime(lesson_date, '%Y-%m') as lesson_month_digits,
    formatDateTime(lesson_date, '%Y %M') AS lesson_month_text,
    toYear(lesson_date) as lesson_year, 
    lesson_date + rand() % 3, -- Дата загрузки данных
    floor(randUniform(2, 137)) as t,
    educational_organization_id * 136 + t as teacher_id,
    floor(t/9) as subject_id,
    CASE subject_id
    	WHEN 1 THEN 'Математика'
    	WHEN 2 THEN 'Русский язык'
    	WHEN 3 THEN 'Литература'
    	WHEN 4 THEN 'Физика'
    	WHEN 5 THEN 'Химия'
    	WHEN 6 THEN 'География'
    	WHEN 7 THEN 'Биология'
    	WHEN 8 THEN 'Физическая культура'
    	ELSE 'Информатика'
    END as subject_name,
    CASE 
    	WHEN randUniform(0, 2) > 1
    		THEN -1
    		ELSE 
    			CASE
	    			WHEN ROUND(randUniform(0, 5)) + subject_id < 5 THEN ROUND(randUniform(4, 5))
	    			WHEN ROUND(randUniform(0, 5)) + subject_id < 9 THEN ROUND(randUniform(3, 5))
	    			ELSE ROUND(randUniform(2, 5))
    			END				
    END AS mark
FROM numbers(10000000);

-- default = 0 - async
select * from system.settings where name= 'mutations_sync';

-- set sync mutations
SET mutations_sync = 1

select count() from mart_student_lesson;

ALTER TABLE learn_db.mart_student_lesson 
	DELETE WHERE educational_organization_id = 2;

SELECT DISTINCT educational_organization_id 
FROM learn_db.mart_student_lesson 
ORDER BY educational_organization_id;




---- DROP PARTITION DEMO

-- recreate table
DROP TABLE learn_db.mart_student_lesson;
CREATE TABLE learn_db.mart_student_lesson
(
	`student_profile_id` Int32, -- Идентификатор профиля обучающегося
	`person_id` String, -- GUID обучающегося
	`person_id_int` Int32 CODEC(Delta, ZSTD),
	`educational_organization_id` Int16, -- Идентификатор образовательной организации
	`parallel_id` Int16,
	`class_id` Int16, -- Идентификатор класса
	`lesson_date` Date32, -- Дата урока
	`lesson_month_digits` String,
	`lesson_month_text` String,
	`lesson_year` UInt16,
	`load_date` Date, -- Дата загрузки данных
	`t` Int16 CODEC(Delta, ZSTD),
	`teacher_id` Int32 CODEC(Delta, ZSTD), -- Идентификатор учителя
	`subject_id` Int16 CODEC(Delta, ZSTD), -- Идентификатор предмета
	`subject_name` String,
	`mark` Int8, -- Оценка
	PRIMARY KEY(lesson_date, person_id_int, mark)
) ENGINE = MergeTree()
PARTITION BY (educational_organization_id)
AS SELECT
	floor(randUniform(2, 1300000)) as student_profile_id,
	cast(student_profile_id as String) as person_id,
	cast(person_id as Int32) as  person_id_int,
    student_profile_id / 365000 as educational_organization_id,
    student_profile_id / 73000 as parallel_id,
    student_profile_id / 2000 as class_id,
    cast(now() - randUniform(2, 60*60*24*365) as date) as lesson_date, -- Дата урока
    formatDateTime(lesson_date, '%Y-%m') as lesson_month_digits,
    formatDateTime(lesson_date, '%Y %M') AS lesson_month_text,
    toYear(lesson_date) as lesson_year, 
    lesson_date + rand() % 3, -- Дата загрузки данных
    floor(randUniform(2, 137)) as t,
    educational_organization_id * 136 + t as teacher_id,
    floor(t/9) as subject_id,
    CASE subject_id
    	WHEN 1 THEN 'Математика'
    	WHEN 2 THEN 'Русский язык'
    	WHEN 3 THEN 'Литература'
    	WHEN 4 THEN 'Физика'
    	WHEN 5 THEN 'Химия'
    	WHEN 6 THEN 'География'
    	WHEN 7 THEN 'Биология'
    	WHEN 8 THEN 'Физическая культура'
    	ELSE 'Информатика'
    END as subject_name,
    CASE 
    	WHEN randUniform(0, 2) > 1
    		THEN -1
    		ELSE 
    			CASE
	    			WHEN ROUND(randUniform(0, 5)) + subject_id < 5 THEN ROUND(randUniform(4, 5))
	    			WHEN ROUND(randUniform(0, 5)) + subject_id < 9 THEN ROUND(randUniform(3, 5))
	    			ELSE ROUND(randUniform(2, 5))
    			END				
    END AS mark
FROM numbers(10000000);


alter table mart_student_lesson
	drop partition '2';

-- show parts
select * 
from system.parts
where table = 'mart_student_lesson'
and active;
-- show not deleted parts in terminal
-- there are unmerged parts and also deleted that take space
-- 459M before flush



--- DETACH PARTITION

-- show active parts for table
select * 
from system.parts
where table = 'mart_student_lesson'
and active;

alter table mart_student_lesson
	detach partition '3';

-- show parts
select * 
from system.parts
where table = 'mart_student_lesson'
and active;

-- check if no rows
select * from mart_student_lesson
where educational_organization_id = 3;

alter table mart_student_lesson
	attach partition '3';

-- check for rows
select * from mart_student_lesson
where educational_organization_id = 3;

-- show parts
select * 
from system.parts
where table = 'mart_student_lesson'
and active;




--- TTL demo

-- create table
DROP TABLE IF EXISTS learn_db.ttl_demo;
CREATE TABLE learn_db.ttl_demo
(
    id UInt64,
    event_time DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY id
TTL event_time + INTERVAL 60 SECOND;


-- insert some rows
INSERT INTO learn_db.ttl_demo
SELECT
    number,
    now() - INTERVAL (number * 5) SECOND
FROM numbers(20);

-- show expiration time
SELECT
    id,
    event_time,
    now() AS now,
    event_time + INTERVAL 10 SECOND AS expire_at
FROM learn_db.ttl_demo
ORDER BY id;

-- show active parts
SELECT
    name,
    partition,
    rows
FROM system.parts
WHERE table = 'ttl_demo' AND active
ORDER BY name;

-- show data path
SELECT data_paths
FROM system.tables
WHERE name = 'ttl_demo';

-- cd /var/lib/clickhouse/store/<hash>/<hash>/
-- ls -l

-- force ttl
OPTIMIZE TABLE learn_db.ttl_demo FINAL;

-- show active parts
SELECT
    name,
    partition,
    rows
FROM system.parts
WHERE table = 'ttl_demo' AND active
ORDER BY name;






----- UPDATE DEMO


DROP TABLE IF EXISTS learn_db.update_demo;

CREATE TABLE learn_db.update_demo
(
    id UInt64,
    user_id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;


INSERT INTO learn_db.update_demo
SELECT
    number,
    number % 3,
    'sometext'
FROM numbers(10);


select * from update_demo;


-- try full update
alter table learn_db.update_demo
	update value = 'updated'
	where value = 'sometext';

select * from learn_db.update_demo;

select * from system.parts
where table = 'update_demo';



--- show best practice column exchange

-- create table
DROP TABLE IF EXISTS learn_db.update_demo;

CREATE TABLE learn_db.update_demo
(
    id UInt64,
    user_id UInt64,
    value String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

-- add some data
INSERT INTO learn_db.update_demo
SELECT
    number,
    number % 3,
    'sometext'
FROM numbers(10);

--check
select * from learn_db.update_demo;

-- add new column
ALTER TABLE learn_db.update_demo 
	ADD COLUMN value_new String DEFAULT 'some_updated';

-- show
select * from learn_db.update_demo;

-- exchange columns
ALTER TABLE learn_db.update_demo
    RENAME COLUMN value TO value_old,
    RENAME COLUMN value_new TO value,
    DROP COLUMN value_old;

-- show
select * from learn_db.update_demo;





--- delete / update witm *MR engines

-- create table
drop table article_reads;

CREATE TABLE article_reads
(
    `user_id` UInt32,
    `article_id` UInt32,
    `read_to` UInt8,
    `read_start` DateTime,
    `read_end` DateTime,
    `sign` Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (read_start, article_id, user_id);


-- user started to read article
INSERT INTO article_reads
            VALUES(1, 12, 0, now(), now(), 1);


select * from article_reads; 


-- user read 70% article
INSERT INTO article_reads
values
-- erase old value
-- add new
(1, 12, 0, '2023-01-06 15:20:32', now(), -1), 
(1, 12, 70, '2023-01-06 15:20:32', now(), 1); 

-- shows all
select * from article_reads;


-- why max?
SELECT
    article_id,
    user_id,
    max(read_end),
    max(read_to)
FROM article_reads
WHERE sign = 1
GROUP BY
    user_id,
    article_id;



-- why max?
SELECT
    article_id,
    user_id,
    read_end,
    read_to
FROM article_reads
WHERE sign = 1
GROUP BY
    user_id,
    article_id;

--- group dont work if 
--- non group column has no agg func


---
--- upserts

drop table article_reads;
CREATE TABLE article_reads
(
    `user_id` UInt32,
    `article_id` UInt32,
    `read_to` UInt8,
    `read_time` DateTime,
    `version` Int32
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (article_id, user_id);

INSERT INTO article_reads
values
(1, 12, 0, '2023-01-06 15:20:32', 1),
(1, 12, 30, '2023-01-06 15:21:42', 2),
(1, 12, 45, '2023-01-06 15:22:13', 3),
(1, 12, 80, '2023-01-06 15:23:10', 4);



SELECT *
FROM article_reads
WHERE 
	user_id = 1 
	AND 
	article_id = 12
ORDER BY version desc;


