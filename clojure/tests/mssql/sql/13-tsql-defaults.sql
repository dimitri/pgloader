-- T-SQL defaults translated to PostgreSQL:
--   (N'new')             → 'new'::text   (not 'N''new''')
--   ('it''s')            → 'it''s'::text
--   (SYSUTCDATETIME()), (SYSDATETIME()), (GETUTCDATE()) → CURRENT_TIMESTAMP
--   (NEWSEQUENTIALID())  → gen_random_uuid()
SELECT column_name, column_default
FROM   information_schema.columns
WHERE  table_schema = 'shop'
  AND  table_name   = 'tsql_defaults'
  AND  column_name <> 'id'
ORDER  BY ordinal_position;

SELECT count(*) AS rows_loaded FROM shop.tsql_defaults;

-- A new row gets the next identity value (sequence reset after 3 rows) and
-- the translated defaults.
INSERT INTO shop.tsql_defaults DEFAULT VALUES
RETURNING id, status, label,
          created_utc IS NOT NULL AS has_created_utc,
          row_guid IS NOT NULL AS has_row_guid;
