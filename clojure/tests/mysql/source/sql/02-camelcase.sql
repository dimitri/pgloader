-- Verify CamelCase table loaded with mixed-case column names preserved.
SELECT count(*) FROM public."CamelCaseTable";
SELECT column_name
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'CamelCaseTable'
ORDER  BY ordinal_position;
