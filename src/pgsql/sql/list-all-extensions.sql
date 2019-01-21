select nspname, extname
  from pg_extension e
       join pg_namespace n on n.oid = e.extnamespace
 where nspname !~ '^pg_';
