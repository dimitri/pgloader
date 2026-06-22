-- #1645: CHECK constraints migrated via ALTER TABLE ADD CONSTRAINT … CHECK (…)
-- Use pg_constraint (more reliable than information_schema for non-superusers).
SELECT c.conname AS constraint_name,
       pg_get_constraintdef(c.oid) AS check_clause
FROM   pg_constraint c
JOIN   pg_class t   ON t.oid = c.conrelid
JOIN   pg_namespace n ON n.oid = t.relnamespace
WHERE  n.nspname  = 'mysql_unit_full'
  AND  t.relname  = 'salary_check'
  AND  c.contype  = 'c'
ORDER  BY c.conname;
