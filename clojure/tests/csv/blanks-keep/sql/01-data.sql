SELECT f1, f2, (f2 IS NULL) AS f2_null
FROM   public.csv_blanks_keep
ORDER  BY f1;
