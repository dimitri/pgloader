#!/usr/bin/env python3
"""
Regenerate test/sqlite/bad-utf8.db for the SQLite UTF-8 decoding error test
(GitHub issue #1250).

Usage:
    python3 test/sqlite/create-bad-utf8.py
"""
import os
import sqlite3

out = os.path.join(os.path.dirname(__file__), "bad-utf8.db")
if os.path.exists(out):
    os.remove(out)

conn = sqlite3.connect(out)
conn.execute(
    """CREATE TABLE files (
    id       INTEGER PRIMARY KEY,
    filename TEXT
)"""
)

conn.execute("INSERT INTO files VALUES (1, 'valid-file.txt')")
# Row 2: Windows-1252 em-dash (0x96) embedded in an otherwise ASCII filename.
# This byte is invalid UTF-8, so cl-sqlite will raise a decoding error when
# pgloader tries to read this column.
conn.execute(
    "INSERT INTO files VALUES (2, ?)",
    (sqlite3.Binary(b"file\x96name.txt"),),
)
conn.execute("INSERT INTO files VALUES (3, 'another-valid.txt')")
conn.commit()
conn.close()
print(f"Created {out}")
