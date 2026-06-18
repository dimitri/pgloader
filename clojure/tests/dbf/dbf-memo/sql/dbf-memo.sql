SELECT count(*) FROM dbf.dnordoc;
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'dbf' AND table_name = 'dnordoc'
 ORDER BY ordinal_position;
