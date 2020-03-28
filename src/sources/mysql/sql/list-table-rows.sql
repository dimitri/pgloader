-- params: db-name
--         including
--         filter-list-to-where-clause incuding
--         excluding
--         filter-list-to-where-clause excluding
    SELECT table_name,
           cast(data_length/avg_row_length as integer)
      FROM information_schema.tables
    WHERE     table_schema = '~a'
          and table_type = 'BASE TABLE'
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~];
