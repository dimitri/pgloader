select typname,
       (array_agg(amname order by amname <> 'gist', amname <> 'gin'))[1]
  from sys_type
       join sys_opclass on sys_opclass.opcintype = sys_type.oid
       join sys_am on sys_am.oid = sys_opclass.opcmethod
 where substring(typname from 1 for 1) <> '_'
       and not exists
       (
         select amname
           from sys_am am
                join sys_opclass c on am.oid = c.opcmethod
                join sys_type t on c.opcintype = t.oid
          where amname = 'btree' and t.oid = sys_type.oid
       )
group by typname;