SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'sqlite_pk'
ORDER  BY table_name;
