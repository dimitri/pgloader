-- Verify that the MySQL 8.0.13+ functional/expression index on users(LOWER(email))
-- was created in PostgreSQL.  The index definition must contain 'lower'.
-- On MariaDB this index is absent (MySQL-version-gated comment is ignored).
SELECT indexdef LIKE '%lower%' AS has_expression
FROM   pg_indexes
WHERE  schemaname = 'mytest'
  AND  tablename  = 'users'
  AND  indexname  LIKE '%email_lower%';
