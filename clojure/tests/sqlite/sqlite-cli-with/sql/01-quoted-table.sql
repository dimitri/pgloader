-- #1187: --with "quote identifiers" via CLI args (no .load file).
-- Verifies that CamelCase table name is preserved as "CamelTable" in PG,
-- and that the CLI path routes WITH options through the grammar without crashing.
SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'public'
  AND  table_name   = 'CamelTable';
