-- #1066: hex-to-bytea cast — raw_hex column stored as PostgreSQL bytea \x…
SELECT label,
       raw_hex
FROM   mysql_unit_full.hex_binary
ORDER  BY id;
