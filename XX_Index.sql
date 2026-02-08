-- https://habr.com/ru/companies/wildberries/articles/821865/
--- show index content
select * from mergeTreeIndex('test', 'table')
where part_name = 'all_XXXXX'
order by idx asc limit 5;