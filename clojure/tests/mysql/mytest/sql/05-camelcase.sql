-- Verify CamelCase table names and column names are preserved under quote identifiers.
SELECT count(*) FROM mytest."CamelCaseTable";
SELECT column_name
FROM   information_schema.columns
WHERE  table_schema = 'mytest'
  AND  table_name   = 'CamelCaseTable'
ORDER  BY ordinal_position;
