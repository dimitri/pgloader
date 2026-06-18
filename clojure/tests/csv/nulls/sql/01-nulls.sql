SELECT f1, (f2 IS NULL) AS f2_null, f3
FROM   public.csv_nulls
ORDER  BY f1;
