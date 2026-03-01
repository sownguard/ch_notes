

----- 1 - ReplacingMergeTree

-- Удаляем и создаем таблицу orders с движком ReplacingMergeTree
DROP TABLE IF EXISTS learn_db.orders;
CREATE TABLE learn_db.orders (
	order_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32
)
ENGINE = ReplacingMergeTree()
ORDER BY (order_id);


-- Вставляем 4 строки в таблицу, соответствующие одному заказу
INSERT INTO learn_db.orders
(order_id, status, amount, pcs)
VALUES
(1, 'created', 100, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs)
VALUES
(1, 'created', 90, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs)
VALUES
(1, 'created', 80, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs)
VALUES
(1, 'created', 110, 1);


-- Получаем все строки из таблицы заказов и только актуальную строку
SELECT * FROM learn_db.orders o;
SELECT * FROM learn_db.orders o FINAL;


-- 1.1 ReplacingMergeTree with version


-- Пересоздаем таблицу orders, добавив колонку с номером версии строки
DROP TABLE IF EXISTS learn_db.orders;
CREATE TABLE learn_db.orders (
	order_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32,
	version UInt32
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (order_id);


-- Вставляем 4 строки в таблицу, соответствующие одному заказу

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version)
VALUES
(1, 'created', 100, 1, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version)
VALUES
(1, 'created', 90, 1, 3);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version)
VALUES
(1, 'created', 80, 1, 4);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version)
VALUES
(1, 'created', 70, 1, 2);


-- Получаем все строки из таблицы заказов и только актуальную строку

SELECT * FROM orders o;
SELECT * FROM orders o FINAL;

select * from system.parts
where table='orders';

--- 1.3 ReplacingMergeTree + version, is_deleted

-- Пересоздаем таблицу orders, добавив колонку с пометкой, что строка удалена

DROP TABLE IF EXISTS learn_db.orders;
CREATE TABLE learn_db.orders (
	order_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32,
	version UInt32,
	is_deleted UInt8
)
ENGINE = ReplacingMergeTree(version, is_deleted)
ORDER BY (status, order_id);


-- Вставляем 4 строки, меняющие состояние заказа с номером 1

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version, is_deleted)
VALUES
(1, 'created', 100, 1, 1, 0);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version, is_deleted)
VALUES
(1, 'created', 100, 1, 1, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version, is_deleted)
VALUES
(1, 'created', 90, 1, 3, 0);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version, is_deleted)
VALUES
(1, 'created', 80, 1, 4, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, version, is_deleted)
VALUES
(1, 'created', 70, 1, 2, 0);


-- Получаем все строки из таблицы заказов и только актуальную строку

SELECT * FROM orders o;
SELECT * FROM orders o FINAL;





-- в конце?
## На сколько запрос замедляется при использовании FINAL

-- Создаем и наполняем таблицу с движком MergeTree

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
PARTITION BY educational_organization_id
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


-- Создаем и наполняем таблицу с движком ReplacingMergeTree

DROP TABLE IF EXISTS learn_db.mart_student_lesson_replacing_merge_tree;
CREATE TABLE learn_db.mart_student_lesson_replacing_merge_tree
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
	`version` UInt32,
	`is_deleted` UInt8,
	PRIMARY KEY(lesson_date, person_id_int, mark)
) ENGINE = ReplacingMergeTree(version, is_deleted)
PARTITION BY educational_organization_id
AS SELECT
	student_profile_id,
	person_id,
	person_id_int,
	educational_organization_id,
	parallel_id,
	class_id,
	lesson_date,
	lesson_month_digits,
	lesson_month_text,
	lesson_year,
	load_date,
	t,
	teacher_id,
	subject_id,
	subject_name,
	mark,
	1 as version,
	0 as is_deleted
FROM
	learn_db.mart_student_lesson;


-- Считаем количество оценок в таблице с движком MergeTree

SELECT 
	mark, 
	count(*) 
FROM learn_db.mart_student_lesson
GROUP BY
	mark;


-- Считаем количество оценок в таблице с движком ReplacingMergeTree без применения FINAL

SELECT 
	mark, 
	count(*) 
FROM learn_db.mart_student_lesson_replacing_merge_tree
GROUP BY
	mark;


-- Считаем количество оценок в таблице с движком ReplacingMergeTree c применением FINAL

SELECT 
	mark, 
	count(*) 
FROM learn_db.mart_student_lesson_replacing_merge_tree FINAL
GROUP BY
	mark;

-- в среднем х5 замедление







-- https://docs.google.com/spreadsheets/d/1t-eznJDy83UoLGb0bTReDJc86KM9Uv73z9Kl4LWsau4/edit?gid=0#gid=0
--- 2.1. CollapsingMergeTree


truncate table  learn_db.orders;
-- Удаляем и создаем таблицу orders
DROP TABLE IF EXISTS learn_db.orders;
CREATE TABLE learn_db.orders (
	order_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32,
	sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (order_id);


-- Вставляем заказ с номером 1
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 100, 1, 1);

-- Правим сумму заказа со 100 до 90
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 100, 1, -1),
(1, 'created', 90, 1, 1);

-- Правим сумму заказа со 90 до 80
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 90, 1, -1),
(1, 'created', 80, 1, 1);

-- Получаем актуальную строку заказа
SELECT 
	order_id,
	status,
	SUM(amount * sign) AS amount,
	SUM(pcs * sign) AS pcs
FROM orders
GROUP by (order_id, status)
HAVING SUM(sign) > 0;
	
-- Меняем сумму заказа с 80 до 70 с помощью 2ух отдельных запросов
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 80, 1, -1)

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 70, 1, 1);

-- Смотрим строки таблицы orders
SELECT *, _part FROM orders;

SELECT *, _part FROM orders final;

-- Меняем статус заказа
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 70, 1, -1),
(1, 'packed', 70, 1, 1);

-- Удаляем заказа с номером 1
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'packed', 70, 1, -1);





--- Additional 

-- Удаляем и создаем таблицу orders заново. Тип поля pcs изменен на Int32
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
	order_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs Int32,
	sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY (order_id);

-- Добавляем строку заказа и 2 раза меняем сумму заказа
INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', 100, 1, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', -100, -1, -1),
(1, 'created', 90, 1, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign)
VALUES
(1, 'created', -90, -1, -1),
(1, 'created', 80, 1, 1);

-- Получаем актуальную строку заказа
SELECT 
	order_id,
	status,
	SUM(amount) AS amount,
	SUM(pcs) AS pcs
FROM orders
GROUP by (order_id, status)
having SUM(sign) > 0;
	
-- Смотрим на строки таблицы orders
SELECT *, _part  FROM orders;

-- Получаем актуальные строки таблицы orders с применением FINAL
SELECT * FROM orders FINAL;

-- Считаем количество актуальных строк в таблице orders
SELECT 
	SUM(sign)
FROM 
	orders;
	
-- Принудительно оставляем для каждого заказа только одну строку (нежелательная операция)
OPTIMIZE TABLE orders FINAL;






-- 3.1 VersionedCollapsingMergeTree


-- Удаляем и создаем таблицу orders

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
	order_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32,
	sign Int8,
	version UInt32
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (order_id);

-- Вставляем данные

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'created', 100, 1, 1, 1);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'created', 100, 1, -1, 1),
(1, 'created', 90, 1, 1, 2);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'created', 90, 1, -1, 2),
(1, 'created', 80, 1, 1, 3);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'created', 80, 1, -1, 3);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'created', 70, 1, 1, 4);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'created', 70, 1, -1, 4),
(1, 'packed', 70, 1, 1, 5);

INSERT INTO learn_db.orders
(order_id, status, amount, pcs, sign, version)
VALUES
(1, 'packed', 70, 1, -1, 5),
(1, 'packed', 60, 1, 1, 6);


-- Запускаем принудительное слияние всех частей таблицы
OPTIMIZE TABLE orders FINAL;

-- Смотрим содержимое таблицы
SELECT *, _part FROM orders;

-- Запрос определения актуальных строк
SELECT 
	order_id,
	status,
	version, 
	SUM(amount * sign) AS amount,
	SUM(pcs * sign) AS pcs
FROM 
	orders
GROUP BY
	order_id,
	status,
	version
HAVING 
	SUM(sign) > 0;





-- 4.1 Summing Merge Tree

# Применение движка SummingMergeTree

-- Создаем таблицу с движком MergeTree, в которой будут храниться сырые данные

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
	order_id UInt32,
	customer_id UInt32,
	sale_dt Date,
	product_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32
)
ENGINE = MergeTree()
ORDER BY (sale_dt, product_id, customer_id, order_id);


-- Вставляем данные в таблицу с сырыми данными

INSERT INTO learn_db.orders
(order_id, customer_id, sale_dt, product_id, status, amount, pcs)
VALUES
(1, 1, '2025-06-01', 1, 'successed', 100, 1),
(1, 1, '2025-06-01', 2, 'successed', 50, 2);

INSERT INTO learn_db.orders
(order_id, customer_id, sale_dt, product_id, status, amount, pcs)
VALUES
(2, 1, '2025-06-01', 1, 'canceled', 110, 1),
(2, 1, '2025-06-01', 2, 'canceled', 60, 2);

INSERT INTO learn_db.orders
(order_id, customer_id, sale_dt, product_id, status, amount, pcs)
VALUES
(3, 2, '2025-06-01', 1, 'successed', 100, 1);

INSERT INTO learn_db.orders
(order_id, customer_id, sale_dt, product_id, status, amount, pcs)
VALUES
(4, 3, '2025-06-01', 2, 'successed', 25, 1);

INSERT INTO learn_db.orders
(order_id, customer_id, sale_dt, product_id, status, amount, pcs)
VALUES
(5, 1, '2025-06-02', 1, 'successed', 95, 1);

INSERT INTO learn_db.orders
(order_id, customer_id, sale_dt, product_id, status, amount, pcs)
VALUES
(6, 1, '2025-06-02', 1, 'successed', 285, 3);



select * from learn_db.orders;


-- Создаем таблицу с движком SummingMergeTree и ключом сортировки по дате, статусу и продукту

DROP TABLE IF EXISTS orders_summ;
CREATE TABLE orders_summ (
	sale_dt Date,
	product_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32
)
ENGINE = SummingMergeTree()
ORDER BY (sale_dt, status, product_id);


-- Вставляем в таблицу с агрегированными данными данные аналогичные, вставленным в таблицу с сырыми данным 

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', 100, 1),
('2025-06-01', 2, 'successed', 50, 2);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'canceled', 110, 1),
('2025-06-01', 2, 'canceled', 60, 2);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', 100, 1);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 2, 'successed', 25, 1);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-02', 1, 'successed', 95, 1);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-02', 1, 'successed', 285, 3);


-- Смотрим содержимое таблиц

SELECT *, _part FROM orders;
SELECT *, _part FROM orders_summ;


-- Окончательный запрос получения сгруппированных, просуммированных данных

SELECT 
	sale_dt,
	product_id,
	status,
	SUM(amount) as amount,
	SUM(pcs) as pcs
FROM
	orders_summ
GROUP BY
	sale_dt,
	product_id,
	status;


-- Пересоздаем таблицу с агрегированными данными с ключом сортировки по дате и продуту (без статуса)

DROP TABLE IF EXISTS orders_summ;
CREATE TABLE orders_summ (
	sale_dt Date,
	product_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32
)
ENGINE = SummingMergeTree()
ORDER BY (sale_dt, product_id);


-- Вставляем данные

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', 100, 1),
('2025-06-01', 2, 'successed', 50, 2);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'canceled', 110, 1),
('2025-06-01', 2, 'canceled', 60, 2);


-- Делаем принудительное слияние частей и смотрим содержимое
-- внимание! статусы пропали - выбирается рандомное

OPTIMIZE TABLE learn_db.orders_summ FINAL; 
SELECT *, _part FROM orders_summ;


-- Пересоздаем таблицу с агрегированными данными, указав суммирование только по полю amount

DROP TABLE IF EXISTS orders_summ;
CREATE TABLE orders_summ (
	sale_dt Date,
	product_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs UInt32
)
ENGINE = SummingMergeTree(amount)
ORDER BY (sale_dt, status, product_id);


-- Вставляем данные

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', 100, 1),
('2025-06-01', 2, 'successed', 50, 2);

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', 110, 1);





-- Делаем принудительное слияние частей и смотрим содержимое

OPTIMIZE TABLE learn_db.orders_summ FINAL; 
SELECT *, _part FROM orders_summ;
--- pcs не суммируется, берется рандомное


-- Пересоздаем таблицу с агрегированными данными (у поля pcs тип меняем на Int32) 

DROP TABLE IF EXISTS orders_summ;
CREATE TABLE orders_summ (
	sale_dt Date,
	product_id UInt32,
	status String,
	amount Decimal(18, 2),
	pcs Int32
)
ENGINE = SummingMergeTree()
ORDER BY (sale_dt, status, product_id);


-- Вставляем данные

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', 100, 1),

INSERT INTO learn_db.orders_summ
(sale_dt, product_id, status, amount, pcs)
VALUES
('2025-06-01', 1, 'successed', -100, -1),


-- Делаем принудительное слияние частей и смотрим содержимое

OPTIMIZE TABLE learn_db.orders_summ FINAL; 
SELECT *, _part FROM orders_summ;
