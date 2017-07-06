-- params: dbname
--         table-name
  select column_name, data_type
    from information_schema.columns
   where table_schema = '~a' and table_name = '~a'
order by ordinal_position;
