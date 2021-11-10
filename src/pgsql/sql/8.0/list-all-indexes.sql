-- params: including
--         filter-list-to-where-clause for including
--         excluding
--         filter-list-to-where-clause for excluding
  select n.nspname,
         i.relname,
         i.oid,
         rn.nspname,
         r.relname,
         indisprimary,
         indisunique,
         null,
         sys_get_indexdef(indexrelid),
         c.conname,
         sys_get_constraintdef(c.oid)
    from sys_index x
         join sys_class i ON i.oid = x.indexrelid
         join sys_class r ON r.oid = x.indrelid
         join sys_namespace n ON n.oid = i.relnamespace
         join sys_namespace rn ON rn.oid = r.relnamespace
         left join sys_depend d on d.classid = 'sys_class'::regclass
                              and d.objid = i.oid
                              and d.refclassid = 'sys_constraint'::regclass
                              and d.deptype = 'i'
         left join sys_constraint c ON c.oid = d.refobjid
   where n.nspname !~~ '^sys_' and n.nspname <> 'information_schema'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
order by n.nspname, r.relname;
