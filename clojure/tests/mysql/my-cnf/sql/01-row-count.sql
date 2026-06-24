-- Verify that rows were loaded: a non-zero count proves the connection
-- succeeded with credentials drawn from ~/.my.cnf.
-- Tables land in the pgloader_mytest schema (no ALTER SCHEMA in this load file).
SELECT COUNT(*) > 0 AS loaded FROM pgloader_mytest.users;
