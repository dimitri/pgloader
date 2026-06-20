SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'mv'
  AND  table_type   = 'BASE TABLE'
ORDER  BY table_name;
