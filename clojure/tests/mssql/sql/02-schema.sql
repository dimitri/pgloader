-- sequences reset: id column should use nextval
SELECT column_default LIKE 'nextval(%' AS has_sequence
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'customers'
  AND  column_name  = 'id';

-- foreign key preserved
SELECT count(*) AS fkeys
FROM   information_schema.table_constraints
WHERE  table_schema    = 'public'
  AND  table_name      = 'orders'
  AND  constraint_type = 'FOREIGN KEY';
