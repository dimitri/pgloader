-- CHECK constraints for a given table.
-- MySQL 8.0.16+ surfaces these in information_schema.CHECK_CONSTRAINTS.
-- On older servers the JOIN returns no rows, so this is safe to run anywhere.
SELECT tc.TABLE_NAME        AS table_name,
       cc.CONSTRAINT_NAME   AS constraint_name,
       cc.CHECK_CLAUSE      AS check_clause
  FROM information_schema.TABLE_CONSTRAINTS tc
  JOIN information_schema.CHECK_CONSTRAINTS cc
    ON cc.CONSTRAINT_SCHEMA = tc.TABLE_SCHEMA
   AND cc.CONSTRAINT_NAME   = tc.CONSTRAINT_NAME
 WHERE tc.TABLE_SCHEMA      = '~a'
   AND tc.CONSTRAINT_TYPE   = 'CHECK'
 ORDER BY tc.TABLE_NAME, cc.CONSTRAINT_NAME
