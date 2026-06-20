-- :name table-exists :? :1
-- :doc Check whether a table exists in the target PostgreSQL database
SELECT EXISTS (
  SELECT 1
    FROM information_schema.tables
   WHERE table_schema = :schema
     AND table_name   = :table
) AS exists

-- :name table-columns :? :*
-- :doc List column names for a table in the target PostgreSQL database (ordered by position)
SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = :schema
   AND table_name   = :table
 ORDER BY ordinal_position

-- :name table-oid :? :1
-- :doc Fetch the pg_class OID for a table; used to generate stable index names (idx_{oid}_PRIMARY)
SELECT c.oid AS oid
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = :schema
   AND c.relname = :table
