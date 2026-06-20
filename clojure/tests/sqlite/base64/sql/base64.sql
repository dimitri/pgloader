SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = 'storage'
 ORDER BY table_name;
