-- #1594: multi-column FK — verify the constraint was created with BOTH columns.
-- Before the fix, only the last column survived, producing a broken 1-column FK
-- that PostgreSQL would accept but that silently mis-mapped column references.

-- Number of FK constraints from composite_child
SELECT count(*) AS fk_count
FROM   information_schema.table_constraints
WHERE  table_schema    = 'public'
  AND  table_name      = 'composite_child'
  AND  constraint_type = 'FOREIGN KEY';

-- Columns included in that FK, in ordinal order (must be exactly 2)
SELECT kcu.column_name, kcu.ordinal_position
FROM   information_schema.key_column_usage kcu
JOIN   information_schema.table_constraints tc
         ON  tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
WHERE  tc.table_schema    = 'public'
  AND  tc.table_name      = 'composite_child'
  AND  tc.constraint_type = 'FOREIGN KEY'
ORDER BY kcu.ordinal_position;

-- Row count survives referential integrity (all 3 child rows are valid)
SELECT count(*) AS child_rows FROM composite_child;
