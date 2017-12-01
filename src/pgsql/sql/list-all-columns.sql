-- params: table-type-name
--         including
--         filter-list-to-where-clause for including
--         excluding
--         filter-list-to-where-clause for excluding
    select nspname, relname, c.oid, attname,
           t.oid::regtype as type,
           case when atttypmod > 0 then atttypmod - 4 else null end as typmod,
           attnotnull,
           case when atthasdef then def.adsrc end as default
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid and attnum > 0
           left join pg_attrdef def on a.attrelid = def.adrelid
                                   and a.attnum = def.adnum

     where nspname !~~ '^pg_' and n.nspname <> 'information_schema'
           and relkind in (~{'~a'~^, ~})
           ~:[~*~;and (~{~a~^~&~10t or ~})~]
           ~:[~*~;and (~{~a~^~&~10t and ~})~]

  order by nspname, relname, attnum;
