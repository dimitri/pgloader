-- All tables landed in the v4 schema
SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'v4'
  AND  table_type   = 'BASE TABLE'
ORDER  BY table_name;
