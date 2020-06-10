-- params: db-name
--         table-type-name
--         only-tables
--         only-tables
--         including
--         filter-list-to-where-clause incuding
--         excluding
--         filter-list-to-where-clause excluding
  select c.table_name, c.column_name, c.column_comment
    from information_schema.columns c
         join information_schema.tables t using(table_schema, table_name)
   where     c.table_schema = '~a'
         and t.table_type = 'BASE TABLE'
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
order by table_name, ordinal_position;
