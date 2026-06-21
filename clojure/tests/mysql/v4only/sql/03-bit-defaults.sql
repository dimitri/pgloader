-- #1280: bit(N) DEFAULT b'0' passes through; column_default is a bit literal
-- #1403: CURRENT_TIMESTAMP default becomes the keyword (not a quoted string)
SELECT column_name,
       data_type,
       column_default
FROM   information_schema.columns
WHERE  table_schema = 'v4'
  AND  table_name   = 'bit_defaults'
ORDER  BY ordinal_position;
