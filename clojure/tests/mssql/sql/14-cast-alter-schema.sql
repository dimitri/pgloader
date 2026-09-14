-- CAST type nvarchar to citext matches the SQL Server type name, and
-- ALTER SCHEMA 'dbo' RENAME TO 'shop' moves dbo.product_codes to shop.
SELECT table_schema, table_name, column_name, udt_name
FROM   information_schema.columns
WHERE  table_name IN ('product_codes', 'tsql_defaults')
  AND  column_name IN ('code', 'label', 'status')
ORDER  BY table_name, column_name;

-- citext: case-insensitive lookups
SELECT code, label FROM shop.product_codes WHERE code = 'ab-100';
