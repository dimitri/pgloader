-- Verify the target table has the renamed column 'transcript', not 'tx'
SELECT column_name
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name   = 'order_summary'
 ORDER BY ordinal_position;
