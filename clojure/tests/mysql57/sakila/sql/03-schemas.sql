-- Which schemas exist after the migration attempt?
-- CL v3 bug: MATERIALIZE ALL VIEWS fails on film_list_rating type,
-- causing a FATAL that rolls back even the base table DDL.
SELECT schema_name
FROM   information_schema.schemata
WHERE  schema_name IN ('pagila', 'mv')
ORDER  BY schema_name;
