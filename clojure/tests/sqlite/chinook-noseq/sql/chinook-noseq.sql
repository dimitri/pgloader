SELECT table_name, table_type
  FROM information_schema.tables
 WHERE table_schema = 'chinook'
 ORDER BY table_name;
