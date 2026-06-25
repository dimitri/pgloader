-- List all user-defined SEQUENCE objects in the current MS SQL database.
-- IDENTITY columns are never backed by sys.sequences — they use a separate
-- internal engine mechanism — so no filtering is required to exclude them.
  SELECT sc.name                       AS schema_name,
         s.name                        AS sequence_name,
         tp.name                       AS data_type,
         CAST(s.start_value   AS BIGINT) AS start_value,
         CAST(s.increment     AS BIGINT) AS increment_by,
         CAST(s.minimum_value AS BIGINT) AS minimum_value,
         CAST(s.maximum_value AS BIGINT) AS maximum_value,
         CAST(s.current_value AS BIGINT) AS current_value,
         s.is_cycling                  AS is_cycling,
         s.cache_size                  AS cache_size
    FROM sys.sequences  s
    JOIN sys.schemas   sc ON sc.schema_id = s.schema_id
    JOIN sys.types     tp ON tp.user_type_id = s.user_type_id
ORDER BY sc.name, s.name
