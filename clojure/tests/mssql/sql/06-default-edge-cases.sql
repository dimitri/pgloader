-- #1163 int column with '' default: must land as NULL (not syntax error)
-- #1409 CONVERT(datetime,...) default: must land as NULL (not syntax error)
SELECT column_name, column_default
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'default_edge_cases'
  AND  column_name IN ('int_empty_def', 'dt_convert')
ORDER BY column_name;

-- Row inserted with DEFAULT VALUES must exist and not be rejected
SELECT count(*) AS rows_loaded FROM public.default_edge_cases;
