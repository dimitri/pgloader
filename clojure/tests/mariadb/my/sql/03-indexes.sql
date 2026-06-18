-- CL v3 names indexes as idx_{table_oid}_{original_name} to handle
-- original names longer than PostgreSQL's 63-char limit.
-- We normalize the OID to {oid} so the output is stable across container runs.
SELECT regexp_replace(indexname, 'idx_\d+_', 'idx_{oid}_') AS indexname
FROM   pg_indexes
WHERE  schemaname = 'mysql'
  AND  indexname  NOT LIKE '%_pkey'
ORDER  BY regexp_replace(indexname, 'idx_\d+_', '');
