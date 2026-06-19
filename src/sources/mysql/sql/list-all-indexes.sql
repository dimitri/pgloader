-- params: db-name
--         table-type-name
--         only-tables
--         only-tables
--         including
--         filter-list-to-where-clause incuding
--         excluding
--         filter-list-to-where-clause excluding
  SELECT table_name, index_name, index_type,
         sum(non_unique),
         cast(GROUP_CONCAT(column_name order by seq_in_index) as char)
    FROM information_schema.statistics
   WHERE table_schema = '~a'
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
GROUP BY table_name, index_name, index_type;
