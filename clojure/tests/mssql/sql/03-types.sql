-- column types: uniqueidentifier→uuid, float→double precision, decimal→numeric, ntext→text, xml→xml
SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'customers'
  AND  column_name IN ('guid', 'score', 'balance')
ORDER BY column_name;

SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'orders'
  AND  column_name IN ('notes', 'metadata')
ORDER BY column_name;
