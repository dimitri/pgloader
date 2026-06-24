-- Verify PRIMARY KEY constraints: every source table with a PRIMARY KEY must
-- have a pg_constraint entry with contype='p'.  This confirms the
-- ALTER TABLE … ADD PRIMARY KEY USING INDEX step ran (not just the index).
SELECT count(*) AS pk_constraints
FROM   pg_constraint c
JOIN   pg_namespace n ON n.oid = c.connamespace
WHERE  c.contype = 'p'
  AND  n.nspname = 'chinook';

-- Total index count: 11 PK indexes + 22 explicit SQLite indexes (IPK_*, IFK_*,
-- sqlite_autoindex_PlaylistTrack_1) = 33.
SELECT count(*) AS total_indexes
FROM   pg_indexes
WHERE  schemaname = 'chinook';
