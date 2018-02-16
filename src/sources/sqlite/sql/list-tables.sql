SELECT tbl_name
  FROM sqlite_master
 WHERE type='table'
       AND tbl_name <> 'sqlite_sequence'
       ~:[~*~;AND (~{~a~^~&~10t or ~})~]
       ~:[~*~;AND (~{~a~^~&~10t and ~})~]
