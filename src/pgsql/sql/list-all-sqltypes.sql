--
-- get user defined SQL types
--
  select nt.nspname,
         extname,
         typname,
         case when enum.enumtypid is not null
              then array_agg(enum.enumlabel order by enumsortorder)
          end as enumvalues

    from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         left join pg_attribute a on c.oid = a.attrelid and a.attnum > 0
         join pg_type t on t.oid = a.atttypid
         left join pg_namespace nt on nt.oid = t.typnamespace
         left join pg_depend d on d.classid = 'pg_type'::regclass
                              and d.refclassid = 'pg_extension'::regclass
                              and d.objid = t.oid
         left join pg_extension e on refobjid = e.oid
         left join pg_enum enum on enum.enumtypid = t.oid

   where nt.nspname !~~ '^pg_' and nt.nspname <> 'information_schema'
         and n.nspname !~~ '^pg_' and n.nspname <> 'information_schema'
         and c.relkind in ('r', 'f', 'p')
           ~:[~*~;and (~{~a~^~&~10t or ~})~]
           ~:[~*~;and (~{~a~^~&~10t and ~})~]
         and
           (   t.typrelid = 0
            or
               (select c.relkind = 'c'
                 from pg_class c
                where c.oid = t.typrelid)
           )
           and not exists
             (
                select 1
                  from pg_type el
                 where el.oid = t.typelem
                   and el.typarray = t.oid
              )

group by nt.nspname, extname, typname, enumtypid
order by nt.nspname, extname, typname, enumtypid;
