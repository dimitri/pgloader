-- :name tables :? :*
-- :doc List all user tables in the current MySQL database
SELECT TABLE_SCHEMA AS table_schema,
       TABLE_NAME   AS table_name
  FROM information_schema.tables
 WHERE table_schema = DATABASE()
   AND table_type   = 'BASE TABLE'
 ORDER BY table_name

-- :name columns :? :*
-- :doc List columns for a given table
SELECT c.COLUMN_NAME                AS column_name,
       c.COLUMN_TYPE                AS column_type,
       c.IS_NULLABLE               AS is_nullable,
       c.COLUMN_DEFAULT            AS column_default,
       c.EXTRA                     AS extra,
       c.CHARACTER_SET_NAME        AS charset,
       c.COLLATION_NAME            AS collation,
       c.COLUMN_KEY                AS column_key,
       c.ORDINAL_POSITION          AS ordinal_position,
       c.NUMERIC_PRECISION         AS numeric_precision,
       c.NUMERIC_SCALE             AS numeric_scale,
       c.CHARACTER_MAXIMUM_LENGTH  AS char_max_length
  FROM information_schema.columns c
 WHERE c.table_schema = :schema
   AND c.table_name   = :table
 ORDER BY c.ORDINAL_POSITION

-- :name table-pkeys :? :*
-- :doc List primary key columns for a table
SELECT k.COLUMN_NAME AS column_name
  FROM information_schema.key_column_usage k
 WHERE k.table_schema = :schema
   AND k.table_name   = :table
   AND k.constraint_name = 'PRIMARY'
 ORDER BY k.ORDINAL_POSITION

-- :name table-counts :? :*
-- :doc Row counts for all tables
SELECT TABLE_NAME AS table_name,
       TABLE_ROWS AS table_rows
  FROM information_schema.tables
 WHERE table_schema = DATABASE()
   AND table_type   = 'BASE TABLE'
