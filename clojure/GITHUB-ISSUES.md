# pgloader GitHub Issues — v4 Status

This document tracks open/closed pgloader GitHub issues and whether the Clojure v4 rewrite addresses them, partially or fully.

Legend:
- ✅ Fixed — v4 handles this correctly by design
- 🔧 Partial — partially addressed; see note
- ❌ Not fixed — same behaviour as v3 or known gap
- 🚫 Won't fix — out of scope or intentional difference

---

## MySQL / MariaDB source

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #943 | `countdata_template` DDL failures | ✅ | Correct type mapping and DDL generation |
| #1004 | IPv6 hostname parsing | ✅ | URI parser handles `[::1]` notation |
| #1041 | MariaDB: column defaults quoted with single quotes | ✅ | `strip-quotes` applied before zero-date check and DDL |
| #1107 | MySQL ENUM columns produce duplicates | ✅ | Cast rules deduplicated by source definition |
| #1132 | `tinyint(1)` should map to `boolean` | ✅ | `pg-type-for` maps `tinyint(1)` → `boolean` |
| #1176 | `int(N)` with N≥10 should map to `bigint` | ✅ | Matches CL cast rule |
| #1200 | MySQL unsigned integers overflow | ✅ | Unsigned upcast: `smallint unsigned` → `integer`, `int unsigned` → `bigint`, `bigint unsigned` → `numeric` |
| #1213 | ENUM and SET types | ✅ | ENUM → `text`, SET → `text[]` |
| #1230 | FULLTEXT index not supported | 🔧 | FULLTEXT indexes silently skipped (no error, no index created) |
| #1240 | MySQL zero dates (`0000-00-00`) | ✅ | `zero-dates-to-null` transform strips zero dates |
| #1265 | MariaDB detection per connection | ✅ | Detected once at connect time via `@@version_comment`; stored on source |
| #1298 | MySQL geometry types require PostGIS | 🔧 | Geometry types mapped to PostGIS types; PostGIS must be present |
| #1304 | MySQL 8 expression/functional indexes | ✅ | `(lower(col))` expressions preserved in index DDL |
| #1352 | `auto_increment` sequence reset | 🔧 | Reset implemented; `bigserial` columns now included in guard |
| #1378 | LOAD DATABASE with no trailing semicolon fails | ✅ | Grammar now accepts optional trailing semicolon |
| #1401 | CamelCase table/column names with `quote identifiers` | ✅ | Original case preserved via `source-table-name`; COPY uses `quote-id` |

## PostgreSQL target / DDL

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #892  | `pg_get_serial_sequence` second arg should be unquoted | ✅ | Fixed in v3 source; v4 uses unquoted column name |
| #950  | Index naming conflicts when loading multiple schemas | ✅ | OID-based index naming (`idx_{oid}_{name}`) avoids conflicts |
| #1015 | `CREATE TABLE IF NOT EXISTS` vs `DROP TABLE` ordering | ✅ | DROP then CREATE in a single transaction |
| #1055 | PRIMARY KEY via `ADD PRIMARY KEY USING INDEX` | ✅ | CREATE UNIQUE INDEX + ALTER TABLE ADD PRIMARY KEY USING INDEX |
| #1089 | ALTER SCHEMA RENAME not applied | ✅ | `apply-alter-schema` renames schema in catalog before DDL |
| #1140 | ALTER TABLE NAMES MATCHING regex | ✅ | `apply-alter-table` with regex filter + SET SCHEMA / RENAME TO |
| #1185 | Incorrect quoting: all identifiers double-quoted | ✅ | `pg-quote-if-needed` only quotes when Postgres would require it |
| #1319 | FK constraint ordering: referenced table must exist first | ✅ | FKs created in post phase after all tables loaded |

## PostgreSQL-as-source (pgsql → pgsql)

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1060 | LOAD DATABASE from PostgreSQL not documented | 🔧 | Implemented in v4; `pgsql://` URI as source |
| #1120 | Serial / identity columns lose sequence on copy | 🔧 | `auto_increment` detection via `NEXTVAL` in `column_default`; reset-sequences runs |
| #1245 | ARRAY column types | 🔧 | Common array types handled via `pg-array-type->pg`; exotic arrays fall back to `text[]` |

## SQLite source

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1030 | SQLite `strftime` / datetime functions in defaults | ✅ | Detected and stripped from column defaults |
| #1090 | SQLite INTEGER PRIMARY KEY as autoincrement | ✅ | Mapped to `bigserial` |

## File sources (CSV / COPY / Fixed-width / DBF)

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #865  | HTTP(S) fetch for archive files | ✅ | `archive/http-fetch!` downloads to temp file; bytes reported in summary |
| #934  | DBF memo fields (`.dbt` sidecar) | ✅ | Memo field reading implemented |
| #1010 | CSV stdin pipe (`cat file | pgloader`) | ✅ | stdin source supported |
| #1035 | Fixed-width with `NULL IF BLANK` | ✅ | `null-if-blank` transform applied |
| #1072 | LOAD ARCHIVE with sub-commands | ✅ | `run-archive-command` dispatches sub-commands; summary combined |

## Citus

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1195 | DISTRIBUTE ... AS REFERENCE TABLE | 🔧 | Grammar parsed; `create_reference_table()` and `create_distributed_table()` SQL generated; FK backfill (`using col from table`) not yet implemented |

## CLI / configuration

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #800  | `--version` flag | ✅ | `pgloader --version` prints `pgloader v4.0.0` |
| #912  | `--debug` flag separate from `--verbose` | ✅ | `--debug` sets TRACE logging + read/write timing in summary |
| #1005 | INI configuration file | 🚫 | INI config deprecated in v4; use `.load` files |
| #1022 | `--summary` output (CSV / JSON) | ✅ | `--summary file.csv` or `--summary file.json` |
| #1048 | Quiet mode (`--quiet`) | ✅ | `--quiet` sets ERROR log level |

## Summary output

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1070 | Summary missing download / extract timing | ✅ | HTTP fetch and archive extraction shown in `:pre` section |
| #1115 | Summary: COPY Wall-Clock Time | ✅ | Wall-clock time for all COPY operations reported in `:post` |
| #1150 | Summary: over-quoting of lowercase identifiers | ✅ | Only double-quotes identifiers that Postgres would quote |

## MSSQL source

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1025 | MSSQL `identity` columns as autoincrement | ✅ | Mapped to `auto_increment` extra; sequence reset applies |
| #1080 | MSSQL `datetime` / `datetime2` types | ✅ | Mapped to `timestamptz` |

---

## Known open gaps in v4

- **FULLTEXT indexes** — silently dropped; no PostGIS-less alternative
- **Citus FK backfill** (`distribute T using col from other_table`) — Phase 2
- **MATERIALIZE VIEWS (named list)** — grammar parsed, implementation uses old path
- **`utilisateurs__Yvelines2013-06-28`** table copy fails in `my` suite (dash in table name with special encoding)
- **INI config files** — not supported (use `.load` files)
- **GraalVM native binary** — JAR only for now; `pgloader` native binary via GraalVM is planned
