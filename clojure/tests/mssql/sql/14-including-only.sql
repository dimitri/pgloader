-- INCLUDING ONLY TABLE NAMES LIKE 'keep%': only the matching tables are
-- migrated (drop_audit is absent), with their rows and the foreign key
-- between them.
SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'inc'
ORDER  BY table_name;

SELECT count(*) AS keep_orders FROM inc.keep_orders;
SELECT count(*) AS keep_items  FROM inc.keep_items;

SELECT count(*) AS fkeys
FROM   information_schema.table_constraints
WHERE  table_schema    = 'inc'
  AND  constraint_type = 'FOREIGN KEY';
