-- Column types for the races table (legacy "my" schema check).
SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'mytest'
  AND  table_name   = 'races'
ORDER  BY ordinal_position;
