-- #1676: zero-dates-to-null transform applied via the cast rule.
-- 'zero' row had created_at = '0000-00-00 00:00:00' → NULL (1 null in created_at).
-- 'null-upd' had updated_at = NULL in MySQL → also NULL in PG (2 nulls in updated_at).
SELECT COUNT(*) FILTER (WHERE created_at IS NULL) AS zero_created,
       COUNT(*) FILTER (WHERE updated_at IS NULL) AS zero_updated
FROM   mytest.zero_dates_notnull;
