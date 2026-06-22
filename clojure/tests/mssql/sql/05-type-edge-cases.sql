-- #976  datetimeoffset → timestamptz
-- #1445 sql_variant → text
SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'type_edge_cases'
  AND  column_name IN ('dto', 'variant_col')
ORDER BY column_name;

-- #1615/#1619 decimal(28,13) precision preserved — value must not be truncated
SELECT big_decimal FROM public.type_edge_cases ORDER BY id;
