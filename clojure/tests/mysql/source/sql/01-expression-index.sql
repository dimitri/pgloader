-- Verify that MySQL functional/expression index was created in PostgreSQL.
-- The index definition should contain 'lower' (the function expression).
SELECT indexdef LIKE '%lower%' AS has_expression
FROM   pg_indexes
WHERE  schemaname = 'public'
  AND  tablename  = 'users'
  AND  indexname  LIKE '%email_lower%';
