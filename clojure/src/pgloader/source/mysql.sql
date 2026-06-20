-- :name tables :? :*
-- :doc List all user tables in the current MySQL database
SELECT TABLE_SCHEMA   AS table_schema,
       TABLE_NAME     AS table_name,
       TABLE_COMMENT  AS table_comment
  FROM information_schema.tables
 WHERE table_schema = DATABASE()
   AND table_type   = 'BASE TABLE'
 ORDER BY table_name

-- :name columns :? :*
-- :doc List columns for a given table
SELECT COLUMN_NAME             AS column_name,
       COLUMN_TYPE             AS column_type,
       IS_NULLABLE             AS is_nullable,
       COLUMN_DEFAULT          AS column_default,
       EXTRA                   AS extra,
       COLUMN_KEY              AS column_key,
       ORDINAL_POSITION        AS ordinal_position,
       COLUMN_COMMENT          AS column_comment
  FROM information_schema.columns
 WHERE table_schema = :schema
   AND table_name   = :table
 ORDER BY ORDINAL_POSITION

-- :name table-pkeys :? :*
-- :doc List primary key columns for a table
SELECT COLUMN_NAME AS column_name
  FROM information_schema.key_column_usage
 WHERE table_schema = :schema
   AND table_name   = :table
   AND constraint_name = 'PRIMARY'
 ORDER BY ORDINAL_POSITION

-- :name views :? :*
-- :doc List all views in the current MySQL database
SELECT TABLE_NAME AS view_name
  FROM information_schema.tables
 WHERE table_schema = DATABASE()
   AND table_type   = 'VIEW'
 ORDER BY table_name

-- :name table-counts :? :*
-- :doc Row counts for all tables
SELECT TABLE_NAME AS table_name,
        TABLE_ROWS AS table_rows
   FROM information_schema.tables
  WHERE table_schema = DATABASE()
    AND table_type   = 'BASE TABLE'

-- :name table-indexes :? :*
-- :doc Secondary indexes for a table (excludes PRIMARY KEY).
--      MySQL 8.0+ functional indexes have a NULL column_name and a non-NULL
--      expression column; we emit the expression wrapped in parens as the
--      "column" so downstream DDL can detect and handle it correctly.
SELECT index_name     AS index_name,
       index_type     AS index_type,
       SUM(non_unique) AS non_unique,
       CAST(GROUP_CONCAT(
              COALESCE(CONCAT('(', expression, ')'), column_name)
              ORDER BY seq_in_index) AS CHAR) AS columns
  FROM information_schema.statistics
 WHERE table_schema = :schema
   AND table_name   = :table
   AND index_name  <> 'PRIMARY'
 GROUP BY index_name, index_type

-- :name table-indexes-mariadb :? :*
-- :doc Secondary indexes for a table on MariaDB (no expression column).
SELECT index_name     AS index_name,
       index_type     AS index_type,
       SUM(non_unique) AS non_unique,
       CAST(GROUP_CONCAT(column_name ORDER BY seq_in_index) AS CHAR) AS columns
  FROM information_schema.statistics
 WHERE table_schema = :schema
   AND table_name   = :table
   AND index_name  <> 'PRIMARY'
 GROUP BY index_name, index_type

-- :name table-fkeys :? :*
-- :doc Foreign key constraints for a table
SELECT tc.constraint_name                 AS constraint_name,
       CAST(GROUP_CONCAT(k.column_name ORDER BY k.ordinal_position) AS CHAR)
                                           AS cols,
       k.referenced_table_name            AS ftable,
       CAST(GROUP_CONCAT(k.referenced_column_name
                         ORDER BY k.position_in_unique_constraint) AS CHAR)
                                           AS fcols,
       rc.update_rule                     AS update_rule,
       rc.delete_rule                     AS delete_rule
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage k
    ON k.table_schema            = tc.table_schema
   AND k.table_name              = tc.table_name
   AND k.constraint_name         = tc.constraint_name
  JOIN information_schema.referential_constraints rc
    ON rc.constraint_schema      = tc.table_schema
   AND rc.constraint_name        = tc.constraint_name
 WHERE tc.table_schema           = :schema
   AND tc.table_name             = :table
   AND tc.constraint_type        = 'FOREIGN KEY'
   AND k.referenced_table_schema = :schema
 GROUP BY tc.constraint_name, k.referenced_table_name, rc.update_rule, rc.delete_rule

-- :name table-avg-row-length :? :1
-- :doc AVG_ROW_LENGTH estimate from information_schema for chunk-size calculation
SELECT COALESCE(AVG_ROW_LENGTH, 0) AS avg_row_length
  FROM information_schema.TABLES
 WHERE table_schema = :schema
   AND table_name   = :table
