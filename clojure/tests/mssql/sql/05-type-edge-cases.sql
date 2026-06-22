-- #976  datetimeoffset → timestamptz
-- #1445 sql_variant → text
SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'type_edge_cases'
  AND  column_name IN ('dto', 'variant_col')
ORDER BY column_name;
