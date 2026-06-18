SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'mysql'
  AND  table_name   = 'races'
ORDER  BY ordinal_position;
