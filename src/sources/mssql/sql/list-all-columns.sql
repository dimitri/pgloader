-- params: dbname
--         table-type-name
--         including
--         filter-list-to-where-clause including
--         excluding
--         filter-list-to-where-clause excluding
  select c.TABLE_SCHEMA,
         c.TABLE_NAME,
         c.COLUMN_NAME,
         c.DATA_TYPE,
         CASE
         WHEN c.COLUMN_DEFAULT LIKE '((%' AND c.COLUMN_DEFAULT LIKE '%))' THEN
             CASE
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4) = 'newid()' THEN 'GENERATE_UUID'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4) LIKE 'convert(%varchar%,getdate(),%)' THEN 'today'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4) = 'getdate()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4) = 'sysdatetimeoffset()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4) LIKE '''%''' THEN SUBSTRING(c.COLUMN_DEFAULT,4,len(c.COLUMN_DEFAULT)-6)
                 ELSE SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4)
             END
         WHEN c.COLUMN_DEFAULT LIKE '(%' AND c.COLUMN_DEFAULT LIKE '%)' THEN
             CASE
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,2,len(c.COLUMN_DEFAULT)-2) = 'newid()' THEN 'GENERATE_UUID'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,2,len(c.COLUMN_DEFAULT)-2) LIKE 'convert(%varchar%,getdate(),%)' THEN 'today'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,2,len(c.COLUMN_DEFAULT)-2) = 'getdate()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,2,len(c.COLUMN_DEFAULT)-2) = 'sysdatetimeoffset()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT,2,len(c.COLUMN_DEFAULT)-2) LIKE '''%''' THEN SUBSTRING(c.COLUMN_DEFAULT,3,len(c.COLUMN_DEFAULT)-4)
                 ELSE SUBSTRING(c.COLUMN_DEFAULT,2,len(c.COLUMN_DEFAULT)-2)
             END
         ELSE c.COLUMN_DEFAULT
         END,
         c.IS_NULLABLE,
         COLUMNPROPERTY(object_id(c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity'),
         c.CHARACTER_MAXIMUM_LENGTH,
         c.NUMERIC_PRECISION,
         c.NUMERIC_PRECISION_RADIX,
         c.NUMERIC_SCALE,
         c.DATETIME_PRECISION,
         c.CHARACTER_SET_NAME,
         c.COLLATION_NAME

    from INFORMATION_SCHEMA.COLUMNS c
         join INFORMATION_SCHEMA.TABLES t
              on c.TABLE_SCHEMA = t.TABLE_SCHEMA
             and c.TABLE_NAME = t.TABLE_NAME

   where     c.TABLE_CATALOG = '~a'
         and t.TABLE_TYPE = '~a'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]

order by c.table_schema, c.table_name, c.ordinal_position;
