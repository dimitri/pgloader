-- :name tables :? :*
-- :doc List all user tables in the current PostgreSQL database
SELECT table_schema,
       table_name
  FROM information_schema.tables
 WHERE table_type = 'BASE TABLE'
   AND table_schema NOT IN ('pg_catalog', 'information_schema')
 ORDER BY table_schema, table_name

-- :name columns :? :*
-- :doc List columns for a given table
SELECT column_name,
       data_type,
       udt_name,
       is_nullable,
       column_default,
       character_maximum_length,
       numeric_precision,
       numeric_scale,
       datetime_precision,
       ordinal_position
  FROM information_schema.columns
 WHERE table_schema = :schema
   AND table_name   = :table
 ORDER BY ordinal_position

-- :name table-pkeys :? :*
-- :doc List primary key columns for a table
SELECT kcu.column_name
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema    = kcu.table_schema
   AND tc.table_name      = kcu.table_name
 WHERE tc.constraint_type = 'PRIMARY KEY'
   AND tc.table_schema    = :schema
   AND tc.table_name      = :table
 ORDER BY kcu.ordinal_position

-- :name table-indexes :? :*
-- :doc Indexes for a table (excluding PRIMARY KEY)
SELECT i.indexname   AS index_name,
       i.indexdef    AS index_def,
       am.amname     AS index_type,
       i.tablespace  AS tablespace
  FROM pg_indexes i
  JOIN pg_class c ON c.relname = i.indexname
  JOIN pg_am am ON am.oid = c.relam
 WHERE i.schemaname = :schema
   AND i.tablename  = :table
   AND i.indexname NOT IN (
       SELECT constraint_name
         FROM information_schema.table_constraints
        WHERE table_schema = :schema
          AND table_name   = :table
          AND constraint_type = 'PRIMARY KEY'
   )
 ORDER BY i.indexname

-- :name table-index-cols :? :*
-- :doc List columns for a specific index
SELECT a.attname AS column_name,
       i.indoption
  FROM pg_index idx
  JOIN pg_class c ON c.oid = idx.indexrelid
  JOIN pg_attribute a ON a.attrelid = idx.indrelid
                     AND a.attnum = ANY(idx.indkey)
 WHERE c.relname = :index_name
 ORDER BY array_position(idx.indkey, a.attnum)

-- :name table-fkeys :? :*
-- :doc Foreign key constraints for a table
SELECT tc.constraint_name,
       kcu.column_name,
       ccu.table_schema  AS foreign_table_schema,
       ccu.table_name    AS foreign_table_name,
       ccu.column_name   AS foreign_column_name,
       rc.update_rule,
       rc.delete_rule
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage kcu
    ON kcu.constraint_name = tc.constraint_name
   AND kcu.table_schema    = tc.table_schema
   AND kcu.table_name      = tc.table_name
  JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
   AND ccu.table_schema    = tc.table_schema
  JOIN information_schema.referential_constraints rc
    ON rc.constraint_name  = tc.constraint_name
   AND rc.constraint_schema = tc.table_schema
 WHERE tc.constraint_type  = 'FOREIGN KEY'
   AND tc.table_schema     = :schema
   AND tc.table_name       = :table
 ORDER BY tc.constraint_name, kcu.ordinal_position

-- :name table-relpages :? :1
-- :doc Number of 8kB disk pages in a table (pg_class.relpages); used to compute ctid ranges
SELECT c.relpages AS relpages
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = :schema
   AND c.relname = :table
   AND c.relkind = 'r'

-- :name server-version-num :? :1
-- :doc PostgreSQL server_version_num (e.g. 140005 for PG 14.5); used to gate ctid range scans (PG >= 14)
SELECT current_setting('server_version_num')::integer AS version_num

-- :name ctid-where :snip
-- :doc WHERE clause fragment for ctid block-range scans (PostgreSQL 14+).
--      :sql:lo and :sql:hi are substituted as raw SQL tid literals e.g. '(0,0)'::tid
WHERE ctid >= :sql:lo AND ctid < :sql:hi
