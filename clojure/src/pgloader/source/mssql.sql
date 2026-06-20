-- :name tables :? :*
-- :doc List all user tables in the current MS SQL database
SELECT TABLE_SCHEMA,
       TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
 WHERE TABLE_TYPE = 'BASE TABLE'
   AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
 ORDER BY TABLE_SCHEMA, TABLE_NAME

-- :name table-columns :? :*
-- :doc Columns for a given table, with default value normalization
SELECT c.COLUMN_NAME,
       c.DATA_TYPE,
       c.IS_NULLABLE,
       CASE
         WHEN c.COLUMN_DEFAULT LIKE '((%' AND c.COLUMN_DEFAULT LIKE '%))' THEN
             CASE
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4) = 'newid()' THEN 'gen_random_uuid()'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4) LIKE 'convert(%varchar%,getdate(),%)' THEN 'CURRENT_DATE'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4) = 'getdate()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4) = 'sysdatetimeoffset()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4) LIKE '''%''' THEN SUBSTRING(c.COLUMN_DEFAULT, 4, LEN(c.COLUMN_DEFAULT) - 6)
                 ELSE SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4)
             END
         WHEN c.COLUMN_DEFAULT LIKE '(%' AND c.COLUMN_DEFAULT LIKE '%)' THEN
             CASE
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 2, LEN(c.COLUMN_DEFAULT) - 2) = 'newid()' THEN 'gen_random_uuid()'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 2, LEN(c.COLUMN_DEFAULT) - 2) LIKE 'convert(%varchar%,getdate(),%)' THEN 'CURRENT_DATE'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 2, LEN(c.COLUMN_DEFAULT) - 2) = 'getdate()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 2, LEN(c.COLUMN_DEFAULT) - 2) = 'sysdatetimeoffset()' THEN 'CURRENT_TIMESTAMP'
                 WHEN SUBSTRING(c.COLUMN_DEFAULT, 2, LEN(c.COLUMN_DEFAULT) - 2) LIKE '''%''' THEN SUBSTRING(c.COLUMN_DEFAULT, 3, LEN(c.COLUMN_DEFAULT) - 4)
                 ELSE SUBSTRING(c.COLUMN_DEFAULT, 2, LEN(c.COLUMN_DEFAULT) - 2)
             END
         ELSE c.COLUMN_DEFAULT
       END AS COLUMN_DEFAULT,
       c.CHARACTER_MAXIMUM_LENGTH,
       c.NUMERIC_PRECISION,
       c.NUMERIC_SCALE,
       c.DATETIME_PRECISION,
       COLUMNPROPERTY(object_id(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
  FROM INFORMATION_SCHEMA.COLUMNS c
 WHERE c.TABLE_SCHEMA = :schema
   AND c.TABLE_NAME = :table
 ORDER BY c.ORDINAL_POSITION

-- :name table-pkeys :? :*
-- :doc Primary key columns for a table
SELECT kcu.COLUMN_NAME
  FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
   AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
   AND tc.TABLE_NAME = kcu.TABLE_NAME
 WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
   AND tc.TABLE_SCHEMA = :schema
   AND tc.TABLE_NAME = :table
 ORDER BY kcu.ORDINAL_POSITION

-- :name table-indexes :? :*
-- :doc Non-primary-key indexes for a table (one row per column, with optional WHERE filter)
SELECT i.name                                     AS INDEX_NAME,
       i.is_unique                                AS IS_UNIQUE,
       COL_NAME(ic.object_id, ic.column_id)       AS COLUMN_NAME,
       i.filter_definition                        AS FILTER_DEFINITION
  FROM sys.indexes i
  JOIN sys.index_columns ic
    ON ic.object_id = i.object_id
   AND ic.index_id = i.index_id
 WHERE i.object_id = OBJECT_ID(QUOTENAME(:schema) + '.' + QUOTENAME(:table))
   AND i.is_primary_key = 0
   AND i.name IS NOT NULL
 ORDER BY i.name, ic.key_ordinal

-- :name views :? :*
-- :doc List all user views in the current MS SQL database
SELECT TABLE_SCHEMA,
       TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
 WHERE TABLE_TYPE = 'VIEW'
   AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
 ORDER BY TABLE_SCHEMA, TABLE_NAME

-- :name table-fkeys :? :*
-- :doc Foreign key constraints for a table
SELECT KCU1.CONSTRAINT_NAME,
       KCU1.COLUMN_NAME,
       KCU2.TABLE_SCHEMA AS FOREIGN_TABLE_SCHEMA,
       KCU2.TABLE_NAME AS FOREIGN_TABLE_NAME,
       KCU2.COLUMN_NAME AS FOREIGN_COLUMN_NAME,
       RC.UPDATE_RULE,
       RC.DELETE_RULE
  FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
    ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
   AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
   AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
  JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
    ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
   AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
   AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
   AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION
 WHERE KCU1.TABLE_SCHEMA = :schema
   AND KCU1.TABLE_NAME = :table
 ORDER BY KCU1.CONSTRAINT_NAME, KCU1.ORDINAL_POSITION

-- :name table-extended-props :? :*
-- :doc MS_Description extended properties for a table (minor_id=0) and its columns (minor_id>0)
SELECT ep.minor_id,
       COALESCE(c.name, '') AS column_name,
       CAST(ep.value AS NVARCHAR(MAX)) AS comment
  FROM sys.extended_properties ep
  LEFT JOIN sys.columns c
         ON c.object_id = ep.major_id
        AND c.column_id = ep.minor_id
 WHERE ep.major_id = OBJECT_ID(QUOTENAME(:schema) + '.' + QUOTENAME(:table))
   AND ep.name = 'MS_Description'
   AND ep.class = 1
 ORDER BY ep.minor_id
