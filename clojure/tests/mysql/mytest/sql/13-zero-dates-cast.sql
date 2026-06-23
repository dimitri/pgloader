-- #1676: 'and not null' source guard on CAST rules.
-- created_at was DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00' → matched by
--   the "and not null" rule: cast to timestamp, NOT NULL dropped → nullable in PG.
-- updated_at was DATETIME (nullable) DEFAULT '0000-00-00 00:00:00' → NOT matched
--   by the "and not null" rule; fallback rule applies.
-- Both should be 'timestamp without time zone' and nullable in PG.
SELECT column_name, data_type, is_nullable
FROM   information_schema.columns
WHERE  table_schema = 'mytest'
  AND  table_name   = 'zero_dates_notnull'
  AND  column_name  IN ('created_at', 'updated_at')
ORDER  BY column_name;
