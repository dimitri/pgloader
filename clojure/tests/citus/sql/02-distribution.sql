SELECT table_name, citus_table_type, distribution_column
FROM   citus_tables
ORDER  BY table_name;
