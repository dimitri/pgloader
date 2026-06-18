SELECT id, (number IS NULL) AS num_null, data
FROM   public.csv_null_if
ORDER  BY id;
