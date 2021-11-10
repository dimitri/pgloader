--
-- get user defined SQL types
--
  select nt.nspname,
         extname,
         typname,
         case when enum.enumtypid is not null
              then array_agg(enum.enumlabel order by enumsortorder)
          end as enumvalues

    from sys_class c
         join sys_namespace n on n.oid = c.relnamespace
         left join sys_attribute a on c.oid = a.attrelid and a.attnum > 0
         join sys_type t on t.oid = a.atttypid
         left join sys_namespace nt on nt.oid = t.typnamespace
         left join sys_depend d on d.classid = 'sys_type'::regclass
                              and d.refclassid = 'sys_extension'::regclass
                              and d.objid = t.oid
         left join sys_extension e on refobjid = e.oid
         left join sys_enum enum on enum.enumtypid = t.oid

   where nt.nspname !~~ '^sys_' and nt.nspname <> 'information_schema'
         and n.nspname !~~ '^sys_' and n.nspname <> 'information_schema'
         and c.relkind in ('r', 'f', 'p')
           ~:[~*~;and (~{~a~^~&~10t or ~})~]
           ~:[~*~;and (~{~a~^~&~10t and ~})~]
         and
           (   t.typrelid = 0
            or
               (select c.relkind = 'c'
                 from sys_class c
                where c.oid = t.typrelid)
           )
           and not exists
             (
                select 1
                  from sys_type el
                 where el.oid = t.typelem
                   and el.typarray = t.oid
              )

group by nt.nspname, extname, typname, enumtypid
order by nt.nspname, extname, typname, enumtypid;
