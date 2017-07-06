-- params: table-name
  select nspname
    from pg_namespace n
    join pg_class c on n.oid = c.relnamespace
   where c.oid = '~a'::regclass;
