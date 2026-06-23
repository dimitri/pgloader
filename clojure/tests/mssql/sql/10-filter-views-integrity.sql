-- #1578/#1603: indexes and FK must survive MATERIALIZE VIEWS with no INCLUDING ONLY.
-- Before the fix, view names contaminated the nil `including` filter and
-- fetch-indexes returned nothing for real tables, silently dropping indexes/FKs.

-- PK + secondary index on order_items must both be present (2 indexes total)
SELECT count(*) AS item_indexes
FROM   pg_indexes
WHERE  schemaname = 'filtered'
  AND  tablename  = 'order_items';

-- The FK from order_items to products must be preserved
SELECT count(*) AS item_fkeys
FROM   information_schema.table_constraints
WHERE  table_schema    = 'filtered'
  AND  table_name      = 'order_items'
  AND  constraint_type = 'FOREIGN KEY';
