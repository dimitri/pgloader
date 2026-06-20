SELECT id, (number IS NULL) AS number_null, data
FROM   public.csv_nulls
ORDER  BY id;
