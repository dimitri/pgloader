select nspname, extname
  from sys_extension e
       join sys_namespace n on n.oid = e.extnamespace
 where nspname !~ '^sys_';
