select typname,
       (array_agg(amname order by amname <> 'gist', amname <> 'gin'))[1]
  from pg_type
       join pg_opclass on pg_opclass.opcintype = pg_type.oid
       join pg_am on pg_am.oid = pg_opclass.opcmethod
 where substring(typname from 1 for 1) <> '_'
       and not exists
       (
         select amname
           from pg_am am
                join pg_opclass c on am.oid = c.opcmethod
                join pg_type t on c.opcintype = t.oid
          where amname = 'btree' and t.oid = pg_type.oid
       )
group by typname;
