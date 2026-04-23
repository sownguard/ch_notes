# Homework: Data Cleaning Pipeline in ClickHouse (Hacker News)

# 0. Подготовка датасета

Создайте таблицу на основе публичного датасета Hacker News.

Ссылка на датасет - https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.csv.gz



Запрос создания пустой таблицы со случайным распределенем на основе датасета (для получения скрипта создания таблицы)
```sql
-- create empty table
CREATE TABLE hackernews_empty 
ENGINE = MergeTree 
ORDER BY tuple() 
EMPTY 
AS 
SELECT * 
FROM url('https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.csv.gz', 'CSVWithNames');
```

## 0.1. Подготовка
    - Получите SQL запрос, которым создана эта таблица
    - Скорректируйте запрос - измените типы данных, если требуется, выберите ключ распределения, который посчитаете нужным на этом этапе
    - Создайте таблицу обновленным скриптом (далее в заданиях эта таблица обозначена как hackernews_local)

## 0.2. Заливка данных

Полностью залить через SQL запрос не отрабатывает (можно попробовать)

Если скрипт заливки падает - проверьте, сколько строк записалось в таблицу

Если скрипт упал и таблица не пустая - очистите таблицу

Гарантированно залить все данные из внешнего датасета можно следующим образом:
- загрузить файл датасета на машину, где развернут Clickhouse / docker контейнер
- поместить файл в папку /var/lib/clickhouse/user_files

Для этого на VM / в контейнере выполните
  ```bash
    cd /var/lib/clickhouse/user_files
    wget https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/hacknernews.csv.gz
  ```

- Дождитесь завершения загрузки
```bash
HTTP request sent, awaiting response... 200 OK
Length: 4939387203 (4.6G) [text/csv]
Saving to: ‘hacknernews.csv.gz’

hacknernews.csv.gz                 100%[===============================================================>]   4.60G  1.51MB/s    in 48m 42s 

2026-04-21 12:21:03 (1.61 MB/s) - ‘hacknernews.csv.gz’ saved [4939387203/4939387203]
```
- Загрузите данные в созданную таблицу напрямую из файла
```sql
insert into ...
SELECT * 
FROM file('hacknernews.csv.gz', 'CSVWithNames');
```

После этого вы можете пересоздавать таблицу и напонять ее данными столько раз, сколько потребуется для экспериментов, ожидать загрузки датасета больше не требуется. Вставка из файла происходит очень быстро

## 1. Моделирование ошибок в данных

## 1.1. Создайте таблицу для моделирования сырого ingestion слоя
(order by и название таблицы выбраны для примера - называйте таблицу и выбирайте сортировку на ваше усмотрение)
```sql
CREATE TABLE raw_hn
AS hackernews_local
ENGINE = MergeTree
ORDER BY (id, time);
```

Наполните таблицу данными из local таблицы

## 1.2. Дубликаты
Создайте дубли части существующих в таблице записей
Один из возможных вариантов:
```sql
INSERT INTO raw_hn
SELECT *
FROM hackernews_local
WHERE rand() % 10 = 0;
```

## 1.3. Обновления
Создайте "новые версии" для части существующих данных
Один из возможных вариантов:
```sql
INSERT INTO raw_hn
SELECT
    id,
    deleted,
    type,
    by,
    time,
    text,
    dead,
    parent,
    poll,
    kids,
    url,
    score + rand() % 100,
    title,
    parts,
    descendants
FROM hackernews_local
WHERE rand() % 20 = 0;
```

## 1.4. Проверка наличия дублей
```sql
SELECT id, count()
FROM raw_hn
GROUP BY id
HAVING count() > 1
LIMIT 10;
```


# 2.0 "Очистка" данных

## 2.1. Построение "очищенной" таблицы 
Создайте таблицу для хранения актуального состояния данных:

например
```sql
CREATE TABLE hn_clean
(
    id UInt64,
    deleted UInt8,
    type String,
    by String,
    time DateTime,
    text String,
    dead UInt8,
    parent UInt64,
    poll UInt64,
    kids Array(UInt32),
    url String,
    score UInt32,
    title String,
    parts Array(UInt32),
    descendants UInt32,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id;
```

## 2.2. Наполните таблицу данными из raw слоя
!!! поле version должно позволять определить последнюю версию записи

- Проанализируйте, что нужно вставлять в поле version - что-то из существующих колонок? Нужно ли преобразование? Генерировать version при вставке?
- Приведите анализ / примеры - плюсы/минусы - на что может повлиять и как? 
- Корректен ли вообще такой подход? Позволит ли действительно очистить дубликаты / старые изменения?
- Нужно / стоит ли изменить raw слой?

Один из вариантов:
```sql
INSERT INTO hn_clean
SELECT *, toUnixTimestamp(time) AS version
FROM raw_hn;
```

## 2.3. Получение данных из "очищенной" таблицы

Приведите запрос, которым вы получите запросы из "чистой" таблицы?
- Нужен ли FINAL?
- Есть ли дубли без указания FINAL?
- Какая строка считается последней?


# 3.0 Удаление данных
В таблице существует колонка deleted 
Эта колонка имеет бизнес-смысл - флаг удаленной статьи / комментария на сайте

## 3.1. "Удаление" - 1
Как "удалить" эти данные при расчете?
Например для запроса
```sql
SELECT by, sum(score)
FROM hn_clean
<???>
GROUP BY by
ORDER BY sum(score) DESC
LIMIT 10;
```

## 3.2. Удаление - 2
Реализуйте известные вам способы удаления данных

Можно на raw / можно на clean таблице

Поясните, в каких случаях какие способы стоит применять

Что происходит с занимаемым таблицей дисковым пространством при том или ином способе удаления?


## 3.3. Удаление - 3
Реализуйте механизм lightweigh delete используя колонку бизнес-колонку deleted

## 4.0. Удаление - 4
Реализуйте удаление всех комментариев (бизнес сущность внутри набора данных - нужно покопаться в самих данных) с помощью collapsingmergetree движка

Основывайтесь на данных таблицы raw или clean на ваш выбор