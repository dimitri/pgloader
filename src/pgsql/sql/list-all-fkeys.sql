-- params: including (table)
--         filter-list-to-where-clause for including
--         excluding (table)
--         filter-list-to-where-clause for excluding
--         including (ftable)
--         filter-list-to-where-clause for including
--         excluding (ftable)
--         filter-list-to-where-clause for excluding
 select n.nspname, c.relname, nf.nspname, cf.relname as frelname,
        r.oid,
        d.refobjid as pkeyoid,
        conname,
        sys_catalog.sys_get_constraintdef(r.oid, true) as condef,
        (select string_agg(attname, ',')
           from sys_attribute
          where attrelid = r.conrelid
            and array[attnum::integer] <@ conkey::integer[]
        ) as conkey,
        (select string_agg(attname, ',')
           from sys_attribute
          where attrelid = r.confrelid
            and array[attnum::integer] <@ confkey::integer[]
        ) as confkey,
        confupdtype, confdeltype, confmatchtype,
        condeferrable, condeferred
   from sys_catalog.sys_constraint r
        JOIN sys_class c on r.conrelid = c.oid
        JOIN sys_namespace n on c.relnamespace = n.oid
        JOIN sys_class cf on r.confrelid = cf.oid
        JOIN sys_namespace nf on cf.relnamespace = nf.oid
        JOIN sys_depend d on d.classid = 'sys_constraint'::regclass
                        and d.objid = r.oid
                        and d.refobjsubid = 0
   where r.contype = 'f'
         AND c.relkind in ('r', 'f', 'p')
         AND cf.relkind in ('r', 'f', 'p')
         AND n.nspname !~~ '^sys_' and n.nspname <> 'information_schema'
         AND nf.nspname !~~ '^sys_' and nf.nspname <> 'information_schema'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
