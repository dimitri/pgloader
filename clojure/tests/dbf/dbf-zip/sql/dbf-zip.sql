SELECT count(*) FROM dbf.france2016;
SELECT column_name, data_type
  FROM information_schema.columns
 WHERE table_schema = 'dbf' AND table_name = 'france2016'
 ORDER BY ordinal_position;
