-- Non-PK indexes in the mytest schema.
-- CL v3 names indexes as idx_{table_oid}_{original_name} to handle names
-- longer than PostgreSQL's 63-char limit.
-- We normalize the OID to {oid} so output is stable across container runs.
SELECT regexp_replace(indexname, 'idx_\d+_', 'idx_{oid}_') AS indexname
FROM   pg_indexes
WHERE  schemaname = 'mytest'
  AND  indexname  NOT LIKE '%_pkey'
ORDER  BY regexp_replace(indexname, 'idx_\d+_', '');
