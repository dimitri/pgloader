-- All base tables in the mytest schema, sorted alphabetically.
SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'mytest'
  AND  table_type   = 'BASE TABLE'
ORDER  BY table_name;
