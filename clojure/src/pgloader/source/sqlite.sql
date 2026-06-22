-- :name list-tables :? :*
-- :doc List all user tables
SELECT tbl_name AS table_name
  FROM sqlite_master
 WHERE type = 'table'
   AND tbl_name <> 'sqlite_sequence'
 ORDER BY tbl_name

-- :name list-views :? :*
-- :doc List all views
SELECT tbl_name AS table_name
  FROM sqlite_master
 WHERE type = 'view'
 ORDER BY tbl_name

-- :name get-create-table :? :1
-- :doc Get CREATE TABLE SQL for autoincrement detection
SELECT sql
  FROM sqlite_master
 WHERE name = :table

-- :name find-sequence :? :1
-- :doc Check if table has a sequence entry
SELECT seq
  FROM sqlite_sequence
 WHERE name = :table
