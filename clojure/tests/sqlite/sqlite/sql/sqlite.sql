SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = 'sqlite'
 ORDER BY table_name;
