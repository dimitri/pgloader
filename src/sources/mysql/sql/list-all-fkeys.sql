-- params: db-name
--         table-type-name
--         only-tables
--         only-tables
--         including
--         filter-list-to-where-clause incuding
--         excluding
--         filter-list-to-where-clause excluding
SELECT s.table_name, s.constraint_name, s.ft, s.cols, s.fcols,
       rc.update_rule, rc.delete_rule

FROM
 (
  SELECT tc.table_schema, tc.table_name,
         tc.constraint_name, k.referenced_table_name ft,

             group_concat(         k.column_name
                          order by k.ordinal_position) as cols,

             group_concat(         k.referenced_column_name
                          order by k.position_in_unique_constraint) as fcols

        FROM information_schema.table_constraints tc

        LEFT JOIN information_schema.key_column_usage k
               ON k.table_schema = tc.table_schema
              AND k.table_name = tc.table_name
              AND k.constraint_name = tc.constraint_name

      WHERE     tc.table_schema = '~a'
            AND k.referenced_table_schema = '~a'
            AND tc.constraint_type = 'FOREIGN KEY'
           ~:[~*~;and (~{tc.table_name ~a~^ or ~})~]
           ~:[~*~;and (~{tc.table_name ~a~^ and ~})~]

   GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, ft
 ) s
             JOIN information_schema.referential_constraints rc
               ON rc.constraint_schema = s.table_schema
              AND rc.constraint_name = s.constraint_name
              AND rc.table_name = s.table_name;
