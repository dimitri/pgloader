-- params: including
--         filter-list-to-where-clause including
--         excluding
--         filter-list-to-where-clause excluding
    select schema_name(schema_id) as SchemaName,
           o.name as TableName,
           REPLACE(i.name, '.', '_') as IndexName,
           co.[name] as ColumnName,
           i.is_unique,
           i.is_primary_key,
           i.filter_definition

    from sys.indexes i
         join sys.objects o on i.object_id = o.object_id
         join sys.index_columns ic on ic.object_id = i.object_id
             and ic.index_id = i.index_id
         join sys.columns co on co.object_id = i.object_id
             and co.column_id = ic.column_id

   where schema_name(schema_id) not in ('dto', 'sys')
         ~:[~*~;and (~{~a~^ or ~})~]
         ~:[~*~;and (~{~a~^ and ~})~]

order by SchemaName,
         o.[name],
         i.[name],
         ic.is_included_column,
         ic.key_ordinal;
