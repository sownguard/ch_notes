# 2 LAB - PK and Disk Storage

How different PK affects amount of disk space your table use for storage.

Run the following query, which queries the system.parts table and returns the amount of disk space used by your pypi table (from prev. lab):
```sql
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table = 'pypi');
```

The pypi table uses timestamp as the primary key, so that is how the data is sorted on disk. Notice that the uncompressed size is much larger than the compressed size.

Your pypi2 table is sorted by (project, timestamp), where project is a fairly low-cardinality string. Compare the disk space being consumed by pypi2:

```sql
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE '%pypi%')
GROUP BY table;
```

Define a new table named test_pypi that has the same schema and primary key as pypi2, but add COUNTRY_CODE as a second column of the primary key (keep TIMESTAMP as the third column). Insert all the rows from pypi2 into test_pypi.


Check the disk usage of test_pypi (use the query from step 3) and compare it to pypi2. Notice that you get a small improvement in compression.