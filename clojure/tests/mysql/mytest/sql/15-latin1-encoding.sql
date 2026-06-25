-- #1368: verify cp1252 bytes in a latin1 MySQL column arrive as correct Unicode.
-- If pgloader mis-decodes latin1 as iso-8859-1, bytes 0x80-0x9F become C1 control
-- characters (e.g. 0x80→U+0080) rather than their cp1252 code points (€, –, ™).
SELECT label,
       word,
       encode(word::bytea, 'hex') AS hex
  FROM mytest.latin1_encoding
 ORDER BY id;
