
drop table hits_NoPrimaryKey;
CREATE TABLE hits_NoPrimaryKey
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY tuple();


--CREATE TABLE hits_NoPrimaryKey_backup
--(
--    `UserID` UInt32,
--    `URL` String,
--    `EventTime` DateTime
--)
--ENGINE = MergeTree
--PRIMARY KEY tuple();

--INSERT INTO hits_NoPrimaryKey SELECT
--   intHash32(UserID) AS UserID,
--   URL,
--   EventTime
--FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz', 'TSV', 'WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8')
--WHERE URL != '';

-- truncate table hits_NoPrimaryKey;


--insert into hits_NoPrimaryKey
--select  *
--	from hits_NoPrimaryKey_backup;

select count() from hits_NoPrimaryKey;
select count() from hits_NoPrimaryKey_backup;

select * from system.parts
where 
table = 'hits_NoPrimaryKey'
and active;

--OPTIMIZE TABLE hits_NoPrimaryKey FINAL;
--OPTIMIZE TABLE hits_NoPrimaryKey_backup FINAL;



-- DEMO

CREATE TABLE hits_NoPrimaryKey
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY tuple();

select count() from hits_NoPrimaryKey;

-- full scan
explain estimate
SELECT URL, count(URL) AS Count
FROM hits_NoPrimaryKey
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count desc


--- create with PK
CREATE TABLE hits_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime)
SETTINGS 
index_granularity_bytes = 0, -- disable adaptive granularity
compress_primary_key = 0; -- no compression of PK -> can list content of PK

--insert into hits_UserID_URL
--select * from hits_NoPrimaryKey;

--OPTIMIZE TABLE hits_UserID_URL FINAL;

select count() from hits_UserID_URL;

-- 1 granule
explain estimate
SELECT URL, count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count desc


-- alter table hits_UserID_URL drop projection prj_url_userid;

--- check URL filter query in indexed table
explain estimate
SELECT UserID, count(UserID) AS Count
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count desc



-- workarounds 
-- first table 
--CREATE TABLE hits_UserID_URL
--(
--    `UserID` UInt32,
--    `URL` String,
--    `EventTime` DateTime
--)
--ENGINE = MergeTree
--PRIMARY KEY (UserID, URL)
--ORDER BY (UserID, URL)
--SETTINGS 
--index_granularity_bytes = 0, -- disable adaptive granularity
--compress_primary_key = 0;

-- second table
CREATE TABLE hits_URL_UserID
(
    `UserID` UInt32,
    `URL` String,
    `EventTime` DateTime
)
ENGINE = MergeTree
PRIMARY KEY (URL, UserID)
SETTINGS index_granularity_bytes = 0, compress_primary_key = 0;

--insert into hits_URL_UserID
--select * from hits_UserID_URL;
--
--optimize table hits_URL_UserID final;

explain estimate
SELECT UserID, count(UserID) AS Count
FROM hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count desc
-- learn_db	hits_URL_UserID	1	688128	84

explain estimate
SELECT URL, count(URL) AS Count
FROM hits_URL_UserID
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count desc
-- learn_db	hits_URL_UserID	1	12107776	1478

select uniq(URL) as uURL, uniq(UserID) as uUID from hits_URL_UserID;

-- matview

CREATE MATERIALIZED VIEW mv_hits_URL_UserID
ENGINE = MergeTree()
PRIMARY KEY (URL, UserID) -- switched PK
AS SELECT * FROM hits_UserID_URL;
-- populate
insert into mv_hits_URL_UserID
select * from hits_UserID_URL;

optimize table mv_hits_URL_UserID final;

explain estimate
SELECT UserID, count(UserID) AS Count
FROM mv_hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
-- learn_db	.inner_id.a2749057-e1a4-439f-822c-a50430d8a981	1	688128	84

select name, formatReadableSize(total_bytes),  formatReadableSize(total_bytes_uncompressed)
from system.tables 
where name in ('mv_hits_URL_UserID', 'hits_UserID_URL')


-- projection

alter table hits_UserID_URL drop PROJECTION prj_url_userid;

ALTER TABLE hits_UserID_URL
    ADD PROJECTION prj_url_userid
    (
        SELECT *
        ORDER BY (URL, UserID)
    );

ALTER TABLE hits_UserID_URL
    MATERIALIZE PROJECTION prj_url_userid;


select * from system.tables where name='hits_UserID_URL';
select * from system.parts where table='hits_UserID_URL' and active;

explain estimate
SELECT UserID, count(UserID) AS Count
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count desc
-- learn_db	hits_UserID_URL	1	688128	84

explain estimate
SELECT UserID, count(UserID) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY UserID
ORDER BY Count desc
-- learn_db	hits_URL_UserID	1	688128	84



explain estimate
SELECT UserID, count(UserID) AS Count
FROM hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count desc
-- learn_db	hits_URL_UserID	1	688128	84

explain estimate
SELECT URL, count(URL) AS Count
FROM hits_URL_UserID
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count desc

-- with projection
--hits_UserID_URL	402.30 MiB	1.18 GiB
--mv_hits_URL_UserID	185.31 MiB	1.18 GiB

-- without projection
-- hits_UserID_URL	216.93 MiB	1.18 GiB
-- mv_hits_URL_UserID	185.31 MiB	1.18 GiB

select * from system.parts where table = 'hits_UserID_URL' and active;

select * from system.mutations ;


---- PK order

CREATE TABLE hits_URL_UserID_IsRobot
(
    `UserID` UInt32,
    `URL` String,
    `IsRobot` UInt8
)
ENGINE = MergeTree
PRIMARY KEY (URL, UserID, IsRobot);

INSERT INTO hits_URL_UserID_IsRobot SELECT
    intHash32(c11::UInt64) AS UserID,
    c15 AS URL,
    c20 AS IsRobot
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
WHERE URL != '';

SET max_threads = 2;
SET input_format_parallel_parsing = 0;

INSERT INTO hits_URL_UserID_IsRobot
SELECT
    intHash32(c11::UInt64) AS UserID,
    c15 AS URL,
    c20 AS IsRobot
FROM url('https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz')
WHERE c15 != '';

truncate table hits_URL_UserID_IsRobot;

-- in terminal
-- wget https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz
-- xz -d hits_v1.tsv.xz

-- move file to /var/lib/clickhouse/user_files
 
INSERT INTO hits_URL_UserID_IsRobot
SELECT
    intHash32(c11::UInt64) AS UserID,
    c15 AS URL,
    c20 AS IsRobot
FROM file('hits_v1.tsv', TSV)
WHERE c15 != '';


SELECT
    formatReadableQuantity(uniq(URL)) AS cardinality_URL,
    formatReadableQuantity(uniq(UserID)) AS cardinality_UserID,
    formatReadableQuantity(uniq(IsRobot)) AS cardinality_IsRobot
from hits_URL_UserID_IsRobot;


CREATE TABLE hits_IsRobot_UserID_URL
(
    `UserID` UInt32,
    `URL` String,
    `IsRobot` UInt8
)
ENGINE = MergeTree
PRIMARY KEY (IsRobot, UserID, URL);

insert into hits_IsRobot_UserID_URL
select * from hits_URL_UserID_IsRobot;

-- URL			USERID			isROBOT
-- 2.39 million	119.22 thousand	4.00

explain estimate
SELECT count(*)
FROM hits_URL_UserID_IsRobot
WHERE UserID = 112304

explain estimate
SELECT count(*)
FROM hits_IsRobot_UserID_URL
WHERE UserID = 112304


-- compression?
SELECT
    table AS Table,
    name AS Column,
    formatReadableSize(data_uncompressed_bytes) AS Uncompressed,
    formatReadableSize(data_compressed_bytes) AS Compressed,
    round(data_uncompressed_bytes / data_compressed_bytes, 0) AS Ratio
FROM system.columns
WHERE (table = 'hits_URL_UserID_IsRobot' OR table = 'hits_IsRobot_UserID_URL') AND (name = 'IsRobot')
ORDER BY Ratio asc

