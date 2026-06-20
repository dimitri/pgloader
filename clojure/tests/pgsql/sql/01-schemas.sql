SELECT schema_name
  FROM information_schema.schemata
 WHERE schema_name NOT IN ('pg_catalog','pg_toast','information_schema')
   AND schema_name NOT LIKE 'pg_%'
 ORDER BY schema_name;
