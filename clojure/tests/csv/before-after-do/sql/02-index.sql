SELECT indexname FROM pg_indexes
WHERE  tablename = 'csv_before_after'
ORDER  BY indexname;
