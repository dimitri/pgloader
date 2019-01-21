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
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
        (select string_agg(attname, ',')
           from pg_attribute
          where attrelid = r.conrelid
            and array[attnum::integer] <@ conkey::integer[]
        ) as conkey,
        (select string_agg(attname, ',')
           from pg_attribute
          where attrelid = r.confrelid
            and array[attnum::integer] <@ confkey::integer[]
        ) as confkey,
        confupdtype, confdeltype, confmatchtype,
        condeferrable, condeferred
   from pg_catalog.pg_constraint r
        JOIN pg_class c on r.conrelid = c.oid
        JOIN pg_namespace n on c.relnamespace = n.oid
        JOIN pg_class cf on r.confrelid = cf.oid
        JOIN pg_namespace nf on cf.relnamespace = nf.oid
        JOIN pg_depend d on d.classid = 'pg_constraint'::regclass
                        and d.objid = r.oid
                        and d.refobjsubid = 0
   where r.contype = 'f'
         AND c.relkind in ('r', 'f', 'p')
         AND cf.relkind in ('r', 'f', 'p')
         AND n.nspname !~~ '^pg_' and n.nspname <> 'information_schema'
         AND nf.nspname !~~ '^pg_' and nf.nspname <> 'information_schema'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
