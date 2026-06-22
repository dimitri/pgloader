-- #1629: datetime(N) precision preserved as timestamptz(N) / time(N)
-- #1403: CURRENT_TIMESTAMP(6) default becomes current_timestamp (keyword, no parens)
SELECT column_name,
       data_type,
       datetime_precision,
       column_default
FROM   information_schema.columns
WHERE  table_schema = 'mysql_unit_full'
  AND  table_name   = 'type_precision'
ORDER  BY ordinal_position;
