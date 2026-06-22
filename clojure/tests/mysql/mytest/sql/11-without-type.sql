-- #1522: WITHOUT TYPE cast — perms column keeps its pgloader-mapped type
-- (SET → text[]) while set-to-enum-array is applied to convert the value.
SELECT column_name, data_type, udt_name
FROM   information_schema.columns
WHERE  table_schema = 'mytest'
  AND  table_name   = 'set_column'
ORDER  BY ordinal_position;

SELECT id, perms
FROM   mytest.set_column
ORDER  BY id;
