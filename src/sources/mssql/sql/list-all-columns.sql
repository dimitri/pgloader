-- params: dbname
--         table-type-name
--         including
--         filter-list-to-where-clause including
--         excluding
--         filter-list-to-where-clause excluding
  select c.table_schema,
         c.table_name,
         c.column_name,
         c.data_type,
         CASE
         WHEN c.column_default LIKE '((%' AND c.column_default LIKE '%))' THEN
             CASE
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) = 'newid()' THEN 'generate_uuid_v4()'
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) LIKE 'convert(%varchar%,getdate(),%)' THEN 'today'
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) = 'getdate()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) LIKE '''%''' THEN SUBSTRING(c.column_default,4,len(c.column_default)-6)
                 ELSE SUBSTRING(c.column_default,3,len(c.column_default)-4)
             END
         WHEN c.column_default LIKE '(%' AND c.column_default LIKE '%)' THEN
             CASE
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) = 'newid()' THEN 'generate_uuid_v4()'
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) LIKE 'convert(%varchar%,getdate(),%)' THEN 'today'
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) = 'getdate()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) LIKE '''%''' THEN SUBSTRING(c.column_default,3,len(c.column_default)-4)
                 ELSE SUBSTRING(c.column_default,2,len(c.column_default)-2)
             END
         ELSE c.column_default
         END,
         c.is_nullable,
         COLUMNPROPERTY(object_id(c.table_name), c.column_name, 'IsIdentity'),
         c.CHARACTER_MAXIMUM_LENGTH,
         c.NUMERIC_PRECISION,
         c.NUMERIC_PRECISION_RADIX,
         c.NUMERIC_SCALE,
         c.DATETIME_PRECISION,
         c.CHARACTER_SET_NAME,
         c.COLLATION_NAME

    from information_schema.columns c
         join information_schema.tables t
              on c.table_schema = t.table_schema
             and c.table_name = t.table_name

   where     c.table_catalog = '~a'
         and t.table_type = '~a'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]

order by c.table_schema, c.table_name, c.ordinal_position;
