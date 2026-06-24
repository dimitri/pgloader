-- Fix C (#1621): pgsql-to-pgsql must round-trip timestamp(N) precision and
-- 'timestamp without time zone' correctly.  Before the fix:
--   ts6 rendered as 'timestamp without time zone(6)' (invalid DDL)
--   ts_notz rendered as 'timestamptz' (wrong timezone semantics in v4)
--   t3 rendered as 'time without time zone(3)' (invalid DDL)
SELECT column_name, data_type, datetime_precision
FROM   information_schema.columns
WHERE  table_schema = 'timestamps'
  AND  table_name   = 'ts_types'
ORDER  BY ordinal_position;
