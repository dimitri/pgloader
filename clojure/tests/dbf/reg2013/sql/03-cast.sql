-- region and tncc should be numeric types (integer / smallint)
SELECT data_type
FROM   information_schema.columns
WHERE  table_schema = 'dbf'
  AND  table_name   = 'reg2013'
  AND  column_name  IN ('region', 'tncc')
ORDER  BY column_name;
