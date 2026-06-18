-- filtered index should have a WHERE clause
SELECT indexdef LIKE '%WHERE%' AS has_where_clause
FROM   pg_indexes
WHERE  schemaname = 'public'
  AND  tablename  = 'customers'
  AND  indexname  LIKE '%idx_customers_email%';
