-- params: table-name
  select nspname
    from sys_namespace n
    join sys_class c on n.oid = c.relnamespace
   where c.oid = '~a'::regclass;
