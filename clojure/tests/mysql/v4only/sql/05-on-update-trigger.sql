-- #1560/#1196: ON UPDATE CURRENT_TIMESTAMP produces a BEFORE UPDATE trigger
SELECT trigger_name,
       event_manipulation,
       action_timing
FROM   information_schema.triggers
WHERE  trigger_schema = 'v4'
  AND  event_object_table = 'on_update_ts'
ORDER  BY trigger_name;
