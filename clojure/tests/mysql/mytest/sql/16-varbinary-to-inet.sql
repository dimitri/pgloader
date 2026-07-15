-- #1757: varbinary-to-inet — VARBINARY(16) raw IP bytes converted to PostgreSQL inet.
-- 4-byte input → IPv4, 16-byte input → IPv6, NULL → NULL.
SELECT label,
       ip_raw
  FROM mytest.ip_addresses
 ORDER BY id;
