#!/usr/bin/env python3
"""
Regenerate test/sqlite/collision.db for the identifier collision test
(GitHub issue #353).

Two columns in the same table share the same first 63 characters so they
would both truncate to the same PostgreSQL identifier name.  pgloader must
detect this early and exit with a clear error rather than letting PostgreSQL
silently lose data.

Usage:
    python3 test/sqlite/create-collision.py
"""
import os
import sqlite3

PREFIX    = "col_very_long_name_that_exceeds_postgresql_identifier_limit_aaa"
assert len(PREFIX) == 63, f"prefix should be 63 chars, got {len(PREFIX)}"
COL_A = PREFIX + "x"   # 64 chars, truncates to PREFIX
COL_B = PREFIX + "y"   # 64 chars, truncates to PREFIX — collision!
assert len(COL_A) == 64
assert COL_A[:63] == COL_B[:63]

out = os.path.join(os.path.dirname(__file__), "collision.db")
if os.path.exists(out):
    os.remove(out)

conn = sqlite3.connect(out)
conn.execute(
    f"""CREATE TABLE products (
    id    INTEGER PRIMARY KEY,
    name  TEXT    NOT NULL,
    {COL_A} INTEGER,
    {COL_B} INTEGER
)"""
)
conn.execute("INSERT INTO products VALUES (1, 'apple', 10, 20)")
conn.commit()
conn.close()
print(f"Created {out}")
print(f"  colliding columns: {COL_A!r}")
print(f"                     {COL_B!r}")
print(f"  both truncate to:  {PREFIX!r}")
