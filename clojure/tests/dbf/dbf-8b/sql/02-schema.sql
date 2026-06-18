SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'dbf'
  AND  table_name   = 'dbase_8b'
ORDER  BY ordinal_position;
