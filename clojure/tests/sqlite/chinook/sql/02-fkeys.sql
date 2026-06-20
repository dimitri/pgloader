SELECT count(*) AS fkeys
FROM   information_schema.table_constraints
WHERE  constraint_type = 'FOREIGN KEY'
  AND  constraint_schema = 'chinook';
