-- CAST rule guards and options from mytest.load:
--   type decimal when (and (= 18 precision) (= 6 scale)) to "double precision"
--     → only propertydecimal.propertyvalue, not the decimal(18,0) ID
--   type mediumint with extra auto_increment to bigserial → bigint + sequence
--   type varchar to varchar keep typemod → varchar(12)
SELECT table_name,
       column_name,
       data_type,
       character_maximum_length,
       numeric_precision,
       numeric_scale,
       column_default LIKE 'nextval(%' AS has_sequence
FROM   information_schema.columns
WHERE  table_schema = 'mytest'
  AND  table_name IN ('cast_guards', 'propertydecimal')
ORDER  BY table_name, ordinal_position;
