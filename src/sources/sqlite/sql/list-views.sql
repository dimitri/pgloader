SELECT tbl_name
  FROM sqlite_master
 WHERE type='view'
       ~:[~*~;AND (~{~a~^~&~10t or ~})~]
       ~:[~*~;AND (~{~a~^~&~10t and ~})~]
