SELECT column_name FROM information_schema.columns
WHERE  table_schema = 'public' AND table_name = 'csv_target_cols'
ORDER  BY ordinal_position;
