-- params: table-type-name
--         including
--         filter-list-to-where-clause for including
--         excluding
--         filter-list-to-where-clause for excluding
with seqattr as
 (
   select adrelid, 
          adnum,
          pg_get_expr(d.adbin, d.adrelid) as adsrc,
          case when pg_get_expr(d.adbin, d.adrelid) ~~ 'nextval'
               then substring(pg_get_expr(d.adbin, d.adrelid)
                              from '''([^'']+)'''
                    )
               else null
           end as seqname
     from pg_attrdef d
 )
    select nspname, relname, c.oid, attname,
           t.oid::regtype as type,
           case when atttypmod > 0
                then substring(format_type(t.oid, atttypmod) from '\d+(?:,\d+)?')
                else null
            end as typmod,
           attnotnull,
           case when atthasdef
                then pg_get_expr(def.adbin, def.adrelid)
            end as default           ,
           case when s.seqname is not null then 'auto_increment' end as extra
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid and attnum > 0
           left join pg_attrdef def on a.attrelid = def.adrelid
                                   and a.attnum = def.adnum
                                   and a.atthasdef
           left join seqattr s on def.adrelid = s.adrelid
                              and def.adnum = s.adnum

     where nspname !~~ '^pg_' and n.nspname <> 'information_schema'
           and relkind in (~{'~a'~^, ~})
           ~:[~*~;and (~{~a~^~&~10t or ~})~]
           ~:[~*~;and (~{~a~^~&~10t and ~})~]

  order by nspname, relname, attnum;
