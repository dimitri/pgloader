-- MATERIALIZE ALL VIEWS works fully on this environment.
-- Two fixes were required to restore it (both in src/sources/mysql/):
--   1. enum-or-set-name deduplicates by source-def so view columns with the
--      same ENUM values as a base table column reuse the existing sqltype.
--   2. only_full_group_by disabled in /etc/mysql/conf.d/sql-mode.cnf —
--      the sakila schema predates MySQL 5.7.5 (which enabled it by default)
--      and CL pgloader was originally tested against MySQL 5.6.
SELECT count(*) AS tables_in_pagila
FROM   information_schema.tables
WHERE  table_schema = 'pagila'
  AND  table_type   = 'BASE TABLE';

SELECT count(*) AS tables_in_mv
FROM   information_schema.tables
WHERE  table_schema = 'mv'
  AND  table_type   = 'BASE TABLE';
