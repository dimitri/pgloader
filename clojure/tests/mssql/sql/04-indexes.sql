-- filtered index should have a WHERE clause
SELECT indexdef LIKE '%WHERE%' AS has_where_clause
FROM   pg_indexes
WHERE  schemaname = 'public'
  AND  tablename  = 'customers'
  AND  indexname  LIKE '%idx_customers_email%';

-- #1608: index with a dash in its name must survive migration (name contains
-- the original 'idx-dash-test' substring, quoted by pgloader as needed).
SELECT count(*) AS dash_index_exists
FROM   pg_indexes
WHERE  schemaname = 'public'
  AND  tablename  = 'customers'
  AND  indexname  LIKE '%idx-dash-test%';
