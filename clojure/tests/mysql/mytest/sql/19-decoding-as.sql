-- DECODING TABLE NAMES MATCHING 'legacy_notes' AS utf8: UTF-8 bytes stored in
-- latin1 columns arrive decoded, not double-encoded ("Jean-FranÃ§ois").
SELECT id, author, note
  FROM mytest.legacy_notes
 ORDER BY id;
