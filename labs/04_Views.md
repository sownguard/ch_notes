### 0. Подготовка датасета

Создайте таблицу для загрузки датасета
```sql
CREATE TABLE uk_price_paid
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new UInt8,
    duration Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2);
```

Наполните таблицу данными
```sql
INSERT INTO uk_price_paid
SELECT
    toUInt32(price_string) AS price,
    parseDateTimeBestEffortUS(time) AS date,
    splitByChar(' ', postcode)[1] AS postcode1,
    splitByChar(' ', postcode)[2] AS postcode2,
    transform(a, ['T', 'S', 'D', 'F', 'O'], ['terraced', 'semi-detached', 'detached', 'flat', 'other']) AS type,
    b = 'Y' AS is_new,
    transform(c, ['F', 'L', 'U'], ['freehold', 'leasehold', 'unknown']) AS duration,
    addr1,
    addr2,
    street,
    locality,
    town,
    district,
    county
FROM url(
    'http://prod1.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv',
    'CSV',
    'uuid_string String,
    price_string String,
    time String,
    postcode String,
    a String,
    b String,
    c String,
    addr1 String,
    addr2 String,
    street String,
    locality String,
    town String,
    district String,
    county String,
    d String,
    e String'
) 
SETTINGS max_http_get_redirects=10;
```
## (Тут есть вопросы, на которые нужно ответить)
Какой препроцессинг проходят данные:

(!) - Зачем в базовой таблице поля типа LowCardinality?

(!) -  postcode разделяется на две колонки: postcode1 и postcode2. Для чего это делается?


- Преобразование поля time в дату

- Игнорируется поле  UUid, оно не нужно для анализа

(!) - Преобразование полей type и duration к удобному для чтению Enum с помощью функции transform - Зачем использовать Enum, почему не String?


- Преобразование поля is_new из string (Y/N) в UInt8 (0/1) 


### 1. Создание представления


Определите представление (view) на основе таблицы `uk_price_paid`:
  1. view содержит колонки 
  - `date`
  - `price`
  - `addr1`
  - `addr2`
  - `street`
  
  2. Выводит объекты недвижимости, расположенные в городе London 

---

### 2. Запросы к view

2.1 Вычислите среднюю цену объектов недвижимости, проданных в London в 2022 году


2.2 Кол-во строк, которое возвращает view?


2.3 Сопопставьте результаты с базовой таблицей с соответствующими филтьтрами


### 3. Matview

3.1: Создание исходной таблицы
Создайте таблицу sales , в которой будут храниться продажи, с полями:
- date (Date) – дата продажи
- product_id (UInt32) – идентификатор товара
- quantity (UInt32) – количество проданных единиц
- price (Decimal(10, 2)) – цена за единицу
- total_amount (Decimal(10, 2)) – общая сумма продажи (quantity * price)

Движок таблицы: MergeTree. Первичный ключ по полю date.


3.2: Создание агрегированной таблицы


Создайте таблицу sales_daily_stats, в которой будут автоматически обновляться и хранить агрегированные данные по дням:
- date (Date)
- total_quantity (UInt32) – суммарное количество проданных товаров
- total_revenue (Decimal(10, 2)) – суммарная выручка
- avg_price (Decimal(10, 2)) – средняя цена товара за день

Движок таблицы: AggregatingMergeTree. Первичный ключ по полю date.
 

3.3: Создание инкрементального материализованного представления


Создайте инкрементальное материализованное представление sales_daily_stats_mv, которое будет автоматически обновлять агрегированные данные по дням в таблице sales_daily_stats.

Используйте группировку по дням.
 

3.4: Добавьте произвольное число строк в таблицу с продажами sales


Проверьте, что данные появились во всех нужных таблицах

 
3.5: Сгенерируйте 10 000+ строк в таблицу sales.


Проверьте, что данные появились во всех нужных таблицах
 

3.6: Напишите запрос получения суммы продаж за любую дату
- получите сумму продаж за 1 день из таблицы sales_daily_stats
- получите сумму продаж за 1 день из таблицы sales