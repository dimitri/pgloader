-- #1066: hex-to-bytea cast — raw_hex column stored as PostgreSQL bytea \x…
SELECT label,
       raw_hex
FROM   mytest.hex_binary
ORDER  BY id;
