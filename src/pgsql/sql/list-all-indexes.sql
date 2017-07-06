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
         pg_get_indexdef(indexrelid),
         c.conname,
         pg_get_constraintdef(c.oid)
    from pg_index x
         join pg_class i ON i.oid = x.indexrelid
         join pg_class r ON r.oid = x.indrelid
         join pg_namespace n ON n.oid = i.relnamespace
         join pg_namespace rn ON rn.oid = r.relnamespace
         left join pg_constraint c ON c.conindid = i.oid
                                  and c.conrelid = r.oid
                                  -- filter out self-fkeys
                                  and c.confrelid <> r.oid
   where n.nspname !~~ '^pg_' and n.nspname <> 'information_schema'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
order by n.nspname, r.relname
