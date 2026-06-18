-- d column must not exist in target
SELECT column_name FROM information_schema.columns
WHERE  table_schema = 'public' AND table_name = 'csv_column_subset'
ORDER  BY ordinal_position;
