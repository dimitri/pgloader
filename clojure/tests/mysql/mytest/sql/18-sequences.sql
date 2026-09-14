-- AUTO_INCREMENT columns become serial/bigserial columns owning a sequence
-- (int → integer, int unsigned → bigint), and "reset sequences" leaves each
-- sequence so that the next generated id is MAX(id) + 1, or 1 when empty.
SELECT table_name,
       column_name,
       data_type,
       pg_get_serial_sequence(format('%I.%I', table_schema, table_name),
                              column_name) IS NOT NULL AS has_sequence
FROM   information_schema.columns
WHERE  table_schema = 'mytest'
  AND  table_name IN ('empty', 'fcm_batches', 'legacy_notes', 'users')
  AND  column_name = 'id'
ORDER  BY table_name;

-- Next value each sequence will hand out, without consuming it.
SELECT 'empty' AS table_name,
       CASE WHEN is_called THEN last_value + 1 ELSE last_value END AS next_id
  FROM mytest.empty_id_seq
UNION ALL
SELECT 'legacy_notes',
       CASE WHEN is_called THEN last_value + 1 ELSE last_value END
  FROM mytest.legacy_notes_id_seq
UNION ALL
SELECT 'users',
       CASE WHEN is_called THEN last_value + 1 ELSE last_value END
  FROM mytest.users_id_seq;
