-- Verify that pgloader inferred PostgreSQL types from the SQLite JDBC metadata
-- without requiring BEFORE LOAD DO. SQLite INTEGER → integer, TEXT → text,
-- REAL → real.
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name   = 'order_summary'
 ORDER BY ordinal_position;
