SELECT table_name
  FROM information_schema.tables
 WHERE table_schema = 'public'
   AND table_type = 'BASE TABLE'
   AND table_name IN ('products', 'active_products', 'cheap_products')
 ORDER BY table_name;
