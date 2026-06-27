#!/usr/bin/env python3
"""
Regenerate test/sqlite/type-mismatch.db for the on-error-stop hang test
(GitHub issue #1622).

The database has three rows in the "products" table:
  id=1  name='apple'   qty=10           (valid INTEGER)
  id=2  name='banana'  qty='lots-of-it' (TEXT stored in INTEGER column)
  id=3  name='cherry'  qty=5            (valid INTEGER)

SQLite stores 'lots-of-it' as TEXT because it has dynamic typing.  When
pgloader tries to COPY this into a PostgreSQL INTEGER column the server
rejects the data at CopyDone time.

Usage:
    python3 test/sqlite/create-type-mismatch.py
"""
import os
import sqlite3

out = os.path.join(os.path.dirname(__file__), "type-mismatch.db")
if os.path.exists(out):
    os.remove(out)

conn = sqlite3.connect(out)
conn.execute(
    """CREATE TABLE products (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL,
    qty  INTEGER NOT NULL
)"""
)

conn.execute("INSERT INTO products VALUES (1, 'apple',  10)")
# Row 2: TEXT value in an INTEGER column.  SQLite accepts this because of
# dynamic typing; PostgreSQL will reject it when COPY tries to parse it.
conn.execute("INSERT INTO products VALUES (2, 'banana', 'lots-of-it')")
conn.execute("INSERT INTO products VALUES (3, 'cherry', 5)")
conn.commit()
conn.close()
print(f"Created {out}")
