# pgloader GitHub Issues — v4 Status

Tracks open GitHub issues and PRs against the v4 Clojure rewrite.
Updated before merge of PR #1705.

Legend:
- ✅ Fixed — v4 handles this correctly by design
- 🔧 Partial — partially addressed; see note
- ❌ Not fixed — same behaviour as v3 or known gap in v4
- 🚫 Won't fix — out of scope or intentional difference
- 🔁 Superseded — open PR against v3 that v4 renders moot

---

## MySQL / MariaDB source

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #943  | `countdata_template` DDL failures | ✅ | Correct type mapping and DDL generation |
| #1004 | IPv6 hostname parsing | ✅ | URI parser handles `[::1]` notation |
| #1041 | MariaDB: column defaults quoted with single quotes | ✅ | `strip-quotes` applied before zero-date check and DDL |
| #1107 | MySQL ENUM columns produce duplicates | ✅ | Cast rules deduplicated by source definition |
| #1132 | `tinyint(1)` should map to `boolean` | ✅ | `pg-type-for` maps `tinyint(1)` → `boolean` |
| #1176 | `int(N)` with N≥10 should map to `bigint` | ✅ | Matches CL cast rule |
| #1200 | MySQL unsigned integers overflow | ✅ | Unsigned upcast: `smallint unsigned` → `integer`, `int unsigned` → `bigint`, `bigint unsigned` → `numeric` |
| #1213 | ENUM and SET types | ✅ | ENUM → `text`, SET → `text[]` |
| #1230 | FULLTEXT index not supported | ✅ | FULLTEXT indexes translated to GIN tsvector indexes |
| #1240 | MySQL zero dates (`0000-00-00`) | ✅ | `zero-dates-to-null` transform strips zero dates |
| #1265 | MariaDB detection per connection | ✅ | Detected once at connect time via `@@version_comment`; stored on source |
| #1298 | MySQL geometry types require PostGIS | 🔧 | Geometry types mapped to PostGIS types; PostGIS must be present on target |
| #1304 | MySQL 8 expression/functional indexes | ✅ | `(lower(col))` expressions preserved in index DDL |
| #1352 | `auto_increment` sequence reset | 🔧 | Reset implemented; `bigserial` columns included in guard; edge cases may remain |
| #1378 | LOAD DATABASE with no trailing semicolon fails | ✅ | Grammar accepts optional trailing semicolon |
| #1401 | CamelCase table/column names with `quote identifiers` | ✅ | Original case preserved; COPY uses `quote-id` |
| #1442 | MySQL → Postgres: Skip COPY for generated columns | ✅ | Generated columns recreated as `GENERATED ALWAYS AS (expr) STORED` on target; excluded from COPY column list. Expression fetched from `GENERATION_EXPRESSION`. v3 did not detect them and failed on COPY. |
| #1539 | MySQL connection recognition failed | ✅ | URI scheme `mysql://` and `mysql:///` both handled |
| #1570 | `:` in the connection string | ✅ | URI parser doubles colons in user/password per spec |
| #1572 | Backslashes in Enum values not possible | ✅ | No special treatment of backslash in ENUM values in v4 |
| #1592 | MySQL's `utf8mb3` (formerly `utf8`) not supported | ✅ | Charset metadata ignored; data passed through as-is via JDBC |
| #1617 | Quoting identifiers in MySQL to PostgreSQL migration | ✅ | `quote-id` applied consistently to all generated DDL |
| #1641 | MySQL to Postgres creates schema named after database instead of `public` | ✅ | Default target schema is `public`; `ALTER SCHEMA` renames explicitly |
| #1653 | Password containing `@` fails | ✅ | URI parser handles `@@` escape for `@` in passwords |
| #1654 | MySQL DB with hyphens in name — unquoted schema in `search_path` | ✅ | `search_path` eliminated in v4; schema always explicit |
| #1661 | `prefetch rows` overridden by deprecated `batch concurrency` | ✅ | `batch concurrency` not parsed in v4; `prefetch rows` works correctly |

## PostgreSQL target / DDL

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #892  | `pg_get_serial_sequence` second arg should be unquoted | ✅ | Fixed; v4 uses unquoted column name |
| #950  | Index naming conflicts when loading multiple schemas | ✅ | OID-based index naming (`idx_{oid}_{name}`) avoids conflicts |
| #1015 | `CREATE TABLE IF NOT EXISTS` vs `DROP TABLE` ordering | ✅ | DROP then CREATE in a single transaction |
| #1055 | PRIMARY KEY via `ADD PRIMARY KEY USING INDEX` | ✅ | CREATE UNIQUE INDEX + ALTER TABLE ADD PRIMARY KEY USING INDEX |
| #1089 | ALTER SCHEMA RENAME not applied | ✅ | `apply-alter-schema` renames schema in catalog before DDL |
| #1140 | ALTER TABLE NAMES MATCHING regex | ✅ | `apply-alter-table` with regex filter + SET SCHEMA / RENAME TO |
| #1185 | Incorrect quoting: all identifiers double-quoted | ✅ | `pg-quote-if-needed` only quotes when Postgres would require it |
| #1319 | FK constraint ordering: referenced table must exist first | ✅ | FKs created in post phase after all tables loaded |
| #1461 | Cannot rename to schema name with `-` in name | ✅ | Schema name always quoted in DDL output |
| #1490 | Can't use `INCLUDING ONLY TABLE NAMES` and `ALTER SCHEMA...RENAME TO` together | ✅ | Implemented correctly in v4 catalog pipeline |
| #1576 | `max parallel create index` not respected | ✅ | v4 uses a dedicated `ExecutorService` with the correct pool size |
| #1600 | Specify TARGET TABLE with database source type | ✅ | `TARGET TABLE` clause supported in grammar and AST |
| #1693 | Failed to migrate with column named `interval` (reserved word) | ✅ | All identifiers go through `pg-quote-if-needed` which quotes reserved words |

## PostgreSQL-as-source (pgsql → pgsql)

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1060 | LOAD DATABASE from PostgreSQL not documented | 🔧 | Implemented in v4; docs updated in `docs/ref/pgsql.rst` |
| #1120 | Serial / identity columns lose sequence on copy | 🔧 | `reset-sequences` runs; identity detection via `NEXTVAL` in `column_default` |
| #1245 | ARRAY column types | 🔧 | Common array types handled via `pg-array-type->pg`; exotic arrays fall back to `text[]` |
| #1419 | pgsql → pgsql timestamp(6) precision | 🔧 | Timestamp types passed through; typemod preservation depends on source metadata |
| #1550 | Option `rows per range` not working in pgsql → pgsql | ✅ | `rows per range` accepted; `chunk size` (50 MB default) is the primary knob; ctid scan on PG 14+ |
| #1556 | pgsql → pgsql include/exclude logic not working | ✅ | INCLUDING ONLY and EXCLUDING implemented correctly in v4 |
| #1601 | Read-only transactions for PostgreSQL sources | ❌ | Not yet implemented; planned enhancement |

## SQLite source

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1030 | SQLite `strftime` / datetime functions in defaults | ✅ | Detected and stripped from column defaults |
| #1090 | SQLite INTEGER PRIMARY KEY as autoincrement | ✅ | Mapped to `bigserial` |
| #1450 | SQLite NULL/empty integer field loses data | ✅ | NULL passed through correctly via JDBC |
| #1451 | SQLite `keep not null` cast not available | ✅ | `keep not null` / `drop not null` supported in cast rules |
| #1472 | SQLite: check for base64 content in blob | ✅ | `byte-vector-to-hex` and base64 blob transforms available |
| #1486 | `quote identifiers` not applied to FK constraints in SQLite | ✅ | All identifiers in FK DDL go through `quote-id` |
| #1515 | Error loading SQLite if type name contains parentheses | ✅ | SQLite type parser handles `NUMERIC(10,2)` and similar |
| #1517 | Empty AUTOINCREMENT sequence if SQLite table has no rows | ✅ | Sequence reset guarded against empty tables |
| #1523 | pgloader fails to import SQLite with timestamp column | ✅ | Timestamp defaults stripped; type mapped to `timestamptz` |
| #1531 | SQLite: `quote identifiers` / `downcase` not applied to FK constraints | ✅ | Fixed in v4; PR #1531 superseded |
| #1543 | No tables found in GeoPackage/SQLite file | ❌ | GeoPackage geometry metadata not handled; regular tables load fine |
| #1547 | SQLite: primary keys not transferred, unique index fails, reset sequences fail | ✅ | All three fixed by design in v4 |
| #1552 | Cannot import SQLite array columns (`TEXT[]`, `NUMERIC[]`, `BYTE[]`) | 🔧 | Text-stored arrays cast correctly; binary arrays may require explicit cast rule |
| #1577 | SQLite FK constraints ignore `quote identifiers` | ✅ | Fixed in v4; PR #1531 superseded |
| #1607 | SQLite JSONB to Postgres JSONB cast error | ✅ | JSON/JSONB column types mapped and passed through |
| #1631 | `STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')` default causes timestamp error | ✅ | SQLite `strftime` defaults stripped before DDL |
| #1665 | SQLite to PostgreSQL default `'null'` | ✅ | String literal `'null'` in defaults normalized to SQL NULL |
| #1687 | SQLite ignores generated columns | ✅ | `GENERATED ALWAYS AS` columns detected and expression extracted from `sqlite_master`; recreated as `GENERATED ALWAYS AS (expr) STORED` on target |

## File sources (CSV / COPY / Fixed-width / DBF)

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #865  | HTTP(S) fetch for archive files | ✅ | `archive/http-fetch!` downloads to temp file; bytes in summary |
| #934  | DBF memo fields (`.dbt` sidecar) | ✅ | Memo field reading implemented |
| #1010 | CSV stdin pipe (`cat file \| pgloader`) | ✅ | stdin source supported |
| #1035 | Fixed-width with `NULL IF BLANK` | ✅ | `null-if-blank` transform applied |
| #1072 | LOAD ARCHIVE with sub-commands | ✅ | `run-archive-command` dispatches sub-commands; summary combined |

## Citus

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1195 | DISTRIBUTE ... AS REFERENCE TABLE | ✅ | Full support: reference tables, distributed tables, and FK backfill (`distribute T using col from other_table`). `augment-catalog` in `ddl/citus.clj` derives implicit rules by walking the FK graph and generates a JOIN-based SELECT when the distribution key lives in a parent table. |

## MSSQL source

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #304  | MSSQL: handle nvarchar fields properly | ✅ | `nvarchar` / `nchar` / `ntext` mapped to `text`; no encoding issues via JDBC |
| #1025 | MSSQL `identity` columns as autoincrement | ✅ | Mapped to `auto_increment` extra; sequence reset applies |
| #1080 | MSSQL `datetime` / `datetime2` types | ✅ | Mapped to `timestamptz` |
| #1445 | `SYB-MSUDT` unsupported type | ❌ | Non-standard MSSQL UDT types not yet handled |
| #1454 | `CFFI::FOREIGN-ENUM MSSQL::%SYB-VALUE-TYPE` error | ✅ | JVM-based v4 uses JDBC; no CFFI / native Sybase libs |
| #1551 | MSSQL MATERIALIZE VIEWS filters all views not just named ones | ✅ | Named views without SQL def are added to catalog (normal DDL+COPY pipeline); named views with SQL def use read-query path |
| #1582 | MSSQL `money` type to `numeric(19,4)` | ✅ | `money` / `smallmoney` mapped to `numeric(19,4)` |
| #1586 | MSSQL IDENTITY columns not detected outside default schema | ✅ | v4 introspects `INFORMATION_SCHEMA.COLUMNS` with full schema scope; PR #1595 superseded |
| #1590 | Incorrect default cast from MSSQL `int` (auto-increment) to `bigserial` | ✅ | v4 maps 32-bit IDENTITY to `serial`, 64-bit to `bigserial`; PR #1596 superseded |
| #1597 | MSSQL table and field names case insensitive | ✅ | `downcase identifiers` / `quote identifiers` work for MSSQL source |
| #1627 | MSSQL `int` to `bigint` default conversion | ✅ | 32-bit int → `integer`; 64-bit → `bigint`; IDENTITY suffix governs serial selection |
| #1630 | MSSQL table and column comments not migrated | ✅ | `MS_Description` extended properties read from `sys.extended_properties`; emitted as `COMMENT ON TABLE/COLUMN` DDL |
| #1669 | Extra underscore in column names with `snake_case` during data-only MSSQL migration | ✅ | `snake-case` identifier transform implemented correctly in v4 |

## CLI / configuration

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #800  | `--version` flag | ✅ | `pgloader --version` prints `pgloader v4.0.0-<sha>` |
| #912  | `--debug` flag separate from `--verbose` | ✅ | `--debug` sets TRACE logging + read/write timing in summary |
| #1005 | INI configuration file | 🚫 | INI config deprecated in v4; use `.load` files or env vars |
| #1022 | `--summary` output (CSV / JSON) | ✅ | `--summary file.csv` or `--summary file.json` |
| #1048 | Quiet mode (`--quiet`) | ✅ | `--quiet` sets ERROR log level |
| #1463 | `--root-dir` default baked into executable (Windows) | ✅ | v4 uses `java.io.tmpdir` system property; no baked-in path |
| #1475 | Document default sslmode | ✅ | Default is `prefer`; documented in connection string section |
| #1476 | Confusing naming of sslmodes | ✅ | sslmode values aligned with libpq: `disable`, `allow`, `prefer`, `require` |
| #1682 | `--context` defaults to sqlite source type | ✅ | v4 does not use INI context; Mustache vars read from env by default |

## Parallelism / performance

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1405 | `rows per range` / `prefetch rows` in WITH throws error | ✅ | Both accepted in v4 grammar |
| #1487 | Slow CSV uploads with latest version | 🔧 | v4 pipeline is different; batch sizing via `batch rows` / `batch size` |
| #1520 | MySQL 5.7 production DB migration performance | 🔧 | `workers` + `concurrency` + `multiple readers per thread` available in v4 |
| #1583 | MSSQL: how to speed up data copying | 🔧 | `workers` parallelism works; intra-table parallelism for MSSQL is Phase 2 |
| #1589 | Heap Exhausted when migrating large MySQL DB | ✅ | JVM heap managed by `-Xmx`; no lisp heap exhaustion |
| #1623 | Migration stuck on big tables from SQL Server | 🔧 | `multiple readers per thread` not yet implemented for MSSQL source |
| #1648 | Readers and writers lose sync, stop operating on same tables | ✅ | v4 architecture: each reader paired with its own dedicated writer; no shared queue |
| #1680 | MySQL to PostgreSQL heap issues | ✅ | JVM heap does not exhaust like CL heap; tune with `-Xmx` |

## Connection / networking

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1480 | Postgres connection string with parameters errors from load file | ✅ | URI option parameters parsed correctly in v4 |
| #1507 | Ubuntu 22: `libcrypto.so.1.1` not found | ✅ | v4 uses JDBC; no OpenSSL native library dependency |
| #1562 | `trivial-utf-8` invalid byte error | ✅ | v4 uses Java's standard UTF-8 codec via JDBC |
| #1579 | Underscore in hostname not parsed correctly | ✅ | v4 URI parser accepts underscores in hostnames |
| #1584 | MSSQL `host\instance,port` connection string format | ✅ | Use `jdbc:sqlserver://host\instance;port=1436;user=...;password=...` — passed directly to the MSSQL JDBC driver without pgloader-side parsing |
| #1636 | pgsql and SSL | ✅ | sslmode supported: `disable`, `allow`, `prefer`, `require` |
| #1675 | Arabic/Unicode text converted to `?` during SQL Server migration | ✅ | v4 uses JDBC; `nvarchar` columns are read as Java `String` (UTF-16 internally) and written as UTF-8 to PostgreSQL; no encoding loss |
| #1678 | URL-encoded connection string with `&` breaks parser | ✅ | v4 accepts full JDBC URLs (`jdbc:postgresql://host/db?foo=a&bar=b`) passed directly to the driver; ESRAP parser not involved |
| #1685 | Parse error with double dashes/hyphens in hostname | ✅ | Hostname parsed as opaque string; `--` not special |

## Summary output

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #1070 | Summary missing download / extract timing | ✅ | HTTP fetch and archive extraction shown in `:pre` section |
| #1115 | Summary: COPY Wall-Clock Time | ✅ | Wall-clock time for all COPY operations reported |
| #1150 | Summary: over-quoting of lowercase identifiers | ✅ | Only double-quotes identifiers that Postgres would require |
| #1561 | Duration at the summary page is wrong | ✅ | v4 uses `System/nanoTime` monotonic clock for all durations |

## Open PRs against v3 that v4 supersedes

These PRs are open against the v3 (Common Lisp) codebase. v4 addresses the
same underlying problem by design. They may still warrant merging into v3 for
users who stay on v3, but are not needed in v4.

| PR | Title | v4 Status |
|----|-------|-----------|
| #1662 | fix: remove batch concurrency support | ✅ `batch concurrency` not parsed in v4 |
| #1652 | fix: Prevent camelCase-to-colname duplicate underscores | ✅ `snake-case` transform correct in v4 |
| #1596 | fix(mssql): int (32-bit auto-increment) cast to bigserial | ✅ Fixed in v4 by correct IDENTITY type sizing |
| #1595 | fix(mssql): IDENTITY columns not detected outside default schema | ✅ Fixed in v4 via full-schema `INFORMATION_SCHEMA` query |
| #1594 | fix(mssql): only last column in FK definition kept | ✅ v4 FK introspection collects all columns |
| #1531 | fix(sqlite): respect case option for foreign keys | ✅ Fixed in v4 |
| #1509 | Fix reset-sequences crash on case-sensitive column names | ✅ Fixed in v4 |
| #1321 | Downcase names in snake_case mode for SQL keywords | ✅ `snake-case` + `pg-quote-if-needed` handle this |

---

## Known open gaps in v4

Not addressed in this PR; remain as future work:

- **MSSQL non-standard UDT types** (`SYB-MSUDT`) — not handled
- **Multiple readers per table for MSSQL** — `workers` works; intra-table parallelism not yet
- **GraalVM native binary** — JAR only; native image planned
- **INI config files** — intentionally deprecated in v4
- **Read-only transaction mode for PostgreSQL sources** — planned
- **PostgreSQL 18 UUID7 type** — not yet in cast rules
- **GeoPackage/SQLite geometry metadata** — regular tables load; geometry schema does not
