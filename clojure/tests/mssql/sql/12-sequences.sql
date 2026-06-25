-- #1497: verify that the MSSQL SEQUENCE was migrated and the NEXT VALUE FOR
-- column default was translated to nextval().

-- The sequence itself must exist in the target schema
SELECT EXISTS (
    SELECT 1
      FROM pg_sequences
     WHERE schemaname = 'public'
       AND sequencename = 'order_seq'
) AS sequence_exists;

-- The column default on seq_orders.id must call nextval()
SELECT pg_get_expr(d.adbin, d.adrelid) LIKE '%nextval%' AS default_uses_nextval
  FROM pg_attribute a
  JOIN pg_attrdef   d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
  JOIN pg_class     c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public'
   AND c.relname = 'seq_orders'
   AND a.attname = 'id';

-- All rows must have been copied
SELECT COUNT(*) = 2 AS rows_copied FROM public.seq_orders;
