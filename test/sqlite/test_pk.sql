--
-- sqlite3 -batch test_pk.db < sqlite_pk.sql
--

PRAGMA foreign_keys = ON;

CREATE TABLE division_kind (
    division_kind_id INTEGER PRIMARY KEY
);

CREATE TABLE division (
    division_id INTEGER PRIMARY KEY,
    division_kind_id INTEGER REFERENCES division_kind(division_kind_id)
);
