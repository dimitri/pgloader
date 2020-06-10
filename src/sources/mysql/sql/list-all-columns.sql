-- params: db-name
--         table-type-name
--         only-tables
--         only-tables
--         including
--         filter-list-to-where-clause incuding
--         excluding
--         filter-list-to-where-clause excluding
  select c.table_name, t.table_comment,
         c.column_name, c.column_comment,
         c.data_type, c.column_type, c.column_default,
         c.is_nullable, c.extra
    from information_schema.columns c
         join information_schema.tables t using(table_schema, table_name)
   where c.table_schema = '~a' and t.table_type = '~a'
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
order by table_name, ordinal_position;
