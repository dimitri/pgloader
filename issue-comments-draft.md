# pgloader WishList — Issue Comment Drafts

Sponsoring hook (same for every issue, do not vary):
> If you'd like to see this feature prioritised, sponsoring its development is
> possible through [The Art of PostgreSQL OSS sponsorship
> program](https://oss.theartofpostgresql.com). Sponsored features get
> dedicated implementation time and are tracked to completion; the design above
> is already in place and ready to go.

---

## #1601 — Read-only source transactions for PostgreSQL

> https://github.com/dimitri/pgloader/issues/1601

### Comment

The use-case is clear: running pgloader against a **hot-standby / read replica**
requires that every connection opened on the source side uses a read-only
transaction, since standbys reject any attempt to write (including implicit
advisory locks that some drivers acquire).

### Design

A new `WITH` option on the source side — `read only` — would set
`SET default_transaction_read_only = on` immediately after opening each source
connection, before any catalog query or data read is issued.

```sql
LOAD DATABASE
     FROM postgresql://user:pass@replica-host/db
     INTO postgresql://user:pass@primary-host/db
WITH read only, include drop, create tables, create indexes;
```

All catalog queries and data reads in the pipeline are already plain `SELECT`s
against the source, so enabling `read only` there is mostly a pure connection
flag — with the following features disabled:

**`MATERIALIZE VIEWS … AS <sql>`** — both v3 and v4 implement this by issuing
`CREATE VIEW … AS <user-sql>` on the **source** connection so that pgloader
can introspect its column types from the source catalog, then reads the rows
and drops the view afterwards.  That `CREATE VIEW` / `DROP VIEW` pair is the
only DDL pgloader ever issues against a PostgreSQL source.  When `read only`
is active, this path must be rejected at parse time with a clear error:

> _"MATERIALIZE VIEWS with an explicit SQL definition requires a writable
> source connection — it creates and drops a temporary view on the source.
> Omit `read only`, or use `MATERIALIZE ALL VIEWS` to work with views that
> already exist on the source."_

**Everything else is unaffected:**

| Feature | Works with `read only` |
|---|---|
| Full database load (tables, indexes, FKs, sequences) | Yes — catalog queries and data reads are plain SELECTs |
| `MATERIALIZE ALL VIEWS` | Yes — reads existing views, no writes to source |
| `ONLY TABLE` / `EXCLUDING TABLE` filters | Yes |
| `CAST` / `TRANSFORM` rules | Yes — applied on the pgloader side |
| `BEFORE LOAD` / `AFTER LOAD` SQL | Yes — those run on the **target**, not the source |

All writes happen on the target connection, which is unaffected.

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #1375 — Abort and rollback when rejection count exceeds a threshold

> https://github.com/dimitri/pgloader/issues/1375

### Comment

Today pgloader counts rejected rows and writes them to a `.rej` file, but it
always finishes the load regardless of how many rows were rejected.  There is
no way to say "if more than N rows are bad, something is wrong with the source
file — stop early."

### Design

Two new `WITH` options that can be used independently or together:

```sql
LOAD CSV FROM 'data.csv' ...
WITH max errors 50,          -- abort after 50 rejected rows (absolute)
     max error ratio 0.01;   -- or after 1 % of rows are rejected
```

The rejection counter is maintained in the batch writer alongside the existing
stats.  When either threshold is crossed the writer signals an abort: the
in-progress `COPY` batch is rolled back and pgloader exits with a non-zero
status and a clear message stating how many rows were rejected and which
threshold was hit.

**Important caveat:** pgloader loads data in batches of ~1 000 rows, each
committed independently.  There is no single wrapping transaction around the
entire load.  When the threshold is crossed, already-committed batches are
**not** rolled back — pgloader aborts forward progress but leaves the target
table in a partially-loaded state.  You get to keep the pieces.  If an
all-or-nothing guarantee is required, wrap the load in a pre/post SQL step
that truncates the table on failure, or use a staging table and swap on
success.

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #1279 — YugabyteDB as a migration target

> https://github.com/dimitri/pgloader/issues/1279

### Comment

YugabyteDB's YSQL layer is wire-compatible with PostgreSQL 15 and accepts
standard `COPY … FROM STDIN`, `CREATE TABLE`, `CREATE INDEX`, and `ALTER TABLE
ADD PRIMARY KEY USING INDEX` without modification.  The Docker image
(`yugabytedb/yugabyte:latest`) is publicly available and usable as a test
target today.

pgloader already has a `connection variant` mechanism used for Redshift; adding
`:yugabyte` as a new variant requires only a small set of targeted adaptations:

| Area | Adaptation |
|---|---|
| Index creation | Omit `CONCURRENTLY`; YugabyteDB rejects it inside a transaction |
| `session_replication_role` | Not supported; skip the FK-disable optimisation |
| Sequences | `RESET SEQUENCES` works; `RESTART WITH` syntax is identical |
| Connection string | `yugabyte://user:pass@host:5433/db` (or detect from port) |

Everything else — DDL generation, COPY streaming, index naming, FK
reinstallation — works as-is.  A dedicated test suite using the public Docker
image would be straightforward to add alongside the existing PostgreSQL suite.

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #732 — Translate MySQL partitioned tables to PostgreSQL declarative partitioning

> https://github.com/dimitri/pgloader/issues/732

### Comment

MySQL supports `RANGE`, `LIST`, `HASH`, and `KEY` partitioning.  PostgreSQL
10+ has native declarative partitioning (`PARTITION BY RANGE/LIST/HASH`).
pgloader currently reads the base table but ignores the partition definition,
producing a single flat table on the target.

### Design

The MySQL catalog already reads from `information_schema`.  The partition
metadata is available in `information_schema.PARTITIONS` and
`information_schema.INNODB_SYS_TABLES`.  The translation:

| MySQL | PostgreSQL |
|---|---|
| `PARTITION BY RANGE (col)` | `PARTITION BY RANGE (col)` + one `CREATE TABLE … PARTITION OF … FOR VALUES FROM … TO …` per partition |
| `PARTITION BY LIST (col)` | `PARTITION BY LIST (col)` + one child per value list |
| `PARTITION BY HASH (col)` | `PARTITION BY HASH (col)` + modulus/remainder children |
| `PARTITION BY KEY (col)` | No direct equivalent — load as a flat table with a warning |

Indexes on the parent propagate automatically on PostgreSQL 11+.  Sub-partitions
(MySQL `SUBPARTITION BY`) map to nested `PARTITION OF` declarations.

Each partition is loaded independently via separate `COPY` commands so that
parallel table loading continues to work at the partition level.

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #510 — Row filtering and multi-table routing for flat-file loads

> https://github.com/dimitri/pgloader/issues/510
> Related: #451, #270 — same design, same implementation unit

### Comment

Three related asks that form a single coherent design: **#270** (skip rows
matching arbitrary predicates, e.g. comment lines that are not valid CSV),
**#451** (route rows from one input file to multiple target tables), and
**#510** (discard rows with a field-value predicate before they enter the
pipeline).  All three live in the same layer of the pipeline and are best
served by a unified syntax extension to the `LOAD CSV`, `LOAD FIXED`, and
`LOAD COPY` commands.

### Design

Two kinds of predicate, distinguished by when they apply in the pipeline:

**Pre-parse filter — `WHEN $line …`**

Operates on the raw input line as a text string, *before* the CSV parser
splits it into fields.  This is the right place to skip comment lines, BOM
markers, separator-only rows, or any line that is not valid CSV — the parser
never sees these lines so there is no field-count mismatch or parse error:

```sql
LOAD CSV FROM 'data.csv' (order_id, customer_id, type, amount)
  WHEN $line NOT STARTING WITH '#'
   AND $line IS NOT EMPTY
  INTO target.orders (order_id, customer_id, amount)
WITH skip header = 1;
```

`$line` is a reserved name that always refers to the raw text line.  For
regex predicates, awk-compatible `/pattern/` syntax is supported as shorthand
for `$line ~ 'pattern'`, with `&&`, `||`, and `!` as boolean operators:

```sql
LOAD CSV FROM 'data.csv' (order_id, customer_id, type, amount)
  WHEN /^[^#]/ && ! /^[[:space:]]*$/
  INTO target.orders (order_id, customer_id, amount)
WITH skip header = 1;
```

Lines that fail the pre-parse filter are counted as "filtered" in the summary —
distinct from "rejected" rows (which fail a type cast).

**Post-parse routing — per-`INTO` `WHEN` clause**

After the CSV parser splits the line into named fields, each `INTO` clause
carries its own optional `WHEN` predicate that operates on field values by
name.  A row is sent to *every* `INTO` whose predicate matches (fan-out).
An `INTO` with no `WHEN` is a catch-all that always fires:

```sql
LOAD CSV FROM 'export.csv' (order_id, customer_id, type, amount)
  WHEN $line NOT STARTING WITH '#'
  INTO target.sales   (order_id, customer_id, amount) WHEN type = 'sale'
  INTO target.refunds (order_id, amount)              WHEN type = 'refund'
  INTO target.archive (order_id, customer_id, type, amount)
WITH skip header = 1;
```

The `WHEN` predicate on an `INTO` clause supports field comparisons (`=`,
`!=`, `~` for regex, `IS NULL`, `IS NOT NULL`) combined with `AND` / `OR`.
Rows that match no `INTO` clause are silently dropped and counted as
"filtered".  The file is read once; each row is broadcast to every matching
writer.  Deduplication is left to the user via `WITH on conflict do nothing`
or a post-load SQL step.

**Backward compatibility**

The existing single-table syntax with no `WHEN` continues to work unchanged —
this is the degenerate case of the new syntax (one `INTO`, no filters):

```sql
LOAD CSV FROM 'data.csv' (col1, col2)
INTO target.table (col1, col2)
WITH ...
```

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #451 — Row filtering and multi-table routing for flat-file loads

> https://github.com/dimitri/pgloader/issues/451
> Related: #510, #270 — same design, same implementation unit

### Comment

_(Same design as #510 — cross-posted because both issues describe aspects of
the same feature.)_

Three related asks that form a single coherent design: **#270** (skip rows
matching arbitrary predicates, e.g. comment lines that are not valid CSV),
**#451** (route rows from one input file to multiple target tables), and
**#510** (discard rows with a field-value predicate before they enter the
pipeline).  All three live in the same layer of the pipeline and are best
served by a unified syntax extension to the `LOAD CSV`, `LOAD FIXED`, and
`LOAD COPY` commands.

### Design

Two kinds of predicate, distinguished by when they apply in the pipeline:

**Pre-parse filter — `WHEN $line …`**

Operates on the raw input line as a text string, *before* the CSV parser
splits it into fields.  This is the right place to skip comment lines, BOM
markers, separator-only rows, or any line that is not valid CSV — the parser
never sees these lines so there is no field-count mismatch or parse error:

```sql
LOAD CSV FROM 'data.csv' (order_id, customer_id, type, amount)
  WHEN $line NOT STARTING WITH '#'
   AND $line IS NOT EMPTY
  INTO target.orders (order_id, customer_id, amount)
WITH skip header = 1;
```

`$line` is a reserved name that always refers to the raw text line.  For
regex predicates, awk-compatible `/pattern/` syntax is supported as shorthand
for `$line ~ 'pattern'`, with `&&`, `||`, and `!` as boolean operators:

```sql
LOAD CSV FROM 'data.csv' (order_id, customer_id, type, amount)
  WHEN /^[^#]/ && ! /^[[:space:]]*$/
  INTO target.orders (order_id, customer_id, amount)
WITH skip header = 1;
```

Lines that fail the pre-parse filter are counted as "filtered" in the summary —
distinct from "rejected" rows (which fail a type cast).

**Post-parse routing — per-`INTO` `WHEN` clause**

After the CSV parser splits the line into named fields, each `INTO` clause
carries its own optional `WHEN` predicate that operates on field values by
name.  A row is sent to *every* `INTO` whose predicate matches (fan-out).
An `INTO` with no `WHEN` is a catch-all that always fires:

```sql
LOAD CSV FROM 'export.csv' (order_id, customer_id, type, amount)
  WHEN $line NOT STARTING WITH '#'
  INTO target.sales   (order_id, customer_id, amount) WHEN type = 'sale'
  INTO target.refunds (order_id, amount)              WHEN type = 'refund'
  INTO target.archive (order_id, customer_id, type, amount)
WITH skip header = 1;
```

The `WHEN` predicate on an `INTO` clause supports field comparisons (`=`,
`!=`, `~` for regex, `IS NULL`, `IS NOT NULL`) combined with `AND` / `OR`.
Rows that match no `INTO` clause are silently dropped and counted as
"filtered".  The file is read once; each row is broadcast to every matching
writer.  Deduplication is left to the user via `WITH on conflict do nothing`
or a post-load SQL step.

**Backward compatibility**

The existing single-table syntax with no `WHEN` continues to work unchanged —
this is the degenerate case of the new syntax (one `INTO`, no filters):

```sql
LOAD CSV FROM 'data.csv' (col1, col2)
INTO target.table (col1, col2)
WITH ...
```

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #342 — SQL output mode: write DDL and data to a file instead of loading

> https://github.com/dimitri/pgloader/issues/342

### Comment

The ask is to run pgloader in a mode where nothing is sent to a live
PostgreSQL server — instead the full DDL and data are written to a file
that can be reviewed, committed to version control, or applied manually.

### Design

A `TO FILE 'output.sql'` target syntax (mirroring `pg_dump`'s plain-text
output mode), or equivalently a `--to-file` CLI flag:

```sql
LOAD DATABASE
     FROM mysql://user:pass@host/db
     TO FILE '/tmp/migration.sql'
WITH create tables, create indexes, reset sequences;
```

In this mode pgloader performs all catalog reads and type-mapping logic as
normal, but instead of opening a PostgreSQL connection it writes to the output
file:

- `CREATE TABLE` / `CREATE INDEX` / `ALTER TABLE` DDL as plain SQL statements
- Data rendered as `COPY table (cols) FROM STDIN … \.` blocks, which
  `psql` accepts directly
- `SELECT setval(…)` calls for sequence resets

The output file is a self-contained script: `psql -f migration.sql` applies the
full migration.  This is particularly useful for inspecting the type-mapping
decisions pgloader makes before committing to a live target.

**Note on pg_dump format:** `pg_dump` also supports a custom binary format and
a tar/directory format that `pg_restore` accepts.  Those formats allow parallel
restore and selective table restore, which would be useful for large migrations.
We would need to study the `pg_dump` file format specification to understand
whether producing a tar- or directory-format output is feasible from pgloader —
it is not ruled out, but it is a separate, more complex feature.

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #270 — Row filtering and multi-table routing for flat-file loads

> https://github.com/dimitri/pgloader/issues/270
> Related: #510, #451 — same design, same implementation unit

### Comment

_(Same design as #510 — cross-posted because both issues describe aspects of
the same feature.)_

Three related asks that form a single coherent design: **#270** (skip rows
matching arbitrary predicates, e.g. comment lines that are not valid CSV),
**#451** (route rows from one input file to multiple target tables), and
**#510** (discard rows with a field-value predicate before they enter the
pipeline).  All three live in the same layer of the pipeline and are best
served by a unified syntax extension to the `LOAD CSV`, `LOAD FIXED`, and
`LOAD COPY` commands.

### Design

Two kinds of predicate, distinguished by when they apply in the pipeline:

**Pre-parse filter — `WHEN $line …`**

Operates on the raw input line as a text string, *before* the CSV parser
splits it into fields.  This is the right place to skip comment lines, BOM
markers, separator-only rows, or any line that is not valid CSV — the parser
never sees these lines so there is no field-count mismatch or parse error:

```sql
LOAD CSV FROM 'data.csv' (order_id, customer_id, type, amount)
  WHEN $line NOT STARTING WITH '#'
   AND $line IS NOT EMPTY
  INTO target.orders (order_id, customer_id, amount)
WITH skip header = 1;
```

`$line` is a reserved name that always refers to the raw text line.  For
regex predicates, awk-compatible `/pattern/` syntax is supported as shorthand
for `$line ~ 'pattern'`, with `&&`, `||`, and `!` as boolean operators:

```sql
LOAD CSV FROM 'data.csv' (order_id, customer_id, type, amount)
  WHEN /^[^#]/ && ! /^[[:space:]]*$/
  INTO target.orders (order_id, customer_id, amount)
WITH skip header = 1;
```

Lines that fail the pre-parse filter are counted as "filtered" in the summary —
distinct from "rejected" rows (which fail a type cast).

**Post-parse routing — per-`INTO` `WHEN` clause**

After the CSV parser splits the line into named fields, each `INTO` clause
carries its own optional `WHEN` predicate that operates on field values by
name.  A row is sent to *every* `INTO` whose predicate matches (fan-out).
An `INTO` with no `WHEN` is a catch-all that always fires:

```sql
LOAD CSV FROM 'export.csv' (order_id, customer_id, type, amount)
  WHEN $line NOT STARTING WITH '#'
  INTO target.sales   (order_id, customer_id, amount) WHEN type = 'sale'
  INTO target.refunds (order_id, amount)              WHEN type = 'refund'
  INTO target.archive (order_id, customer_id, type, amount)
WITH skip header = 1;
```

The `WHEN` predicate on an `INTO` clause supports field comparisons (`=`,
`!=`, `~` for regex, `IS NULL`, `IS NOT NULL`) combined with `AND` / `OR`.
Rows that match no `INTO` clause are silently dropped and counted as
"filtered".  The file is read once; each row is broadcast to every matching
writer.  Deduplication is left to the user via `WITH on conflict do nothing`
or a post-load SQL step.

**Backward compatibility**

The existing single-table syntax with no `WHEN` continues to work unchanged —
this is the degenerate case of the new syntax (one `INTO`, no filters):

```sql
LOAD CSV FROM 'data.csv' (col1, col2)
INTO target.table (col1, col2)
WITH ...
```

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.

---

## #244 — Oracle source support

> https://github.com/dimitri/pgloader/issues/244

### Comment

Oracle → PostgreSQL is the most in-demand enterprise migration path and the one
most conspicuously absent from pgloader's source list.  The Clojure v4
architecture — which talks to every source through JDBC — is substantially more
amenable to adding Oracle than v3 was, where each source required a
protocol-level driver.

### Design

Three things are needed to add Oracle as a pgloader source.  Two of the three
are well-understood; the third is the main body of work.

**1. Driver** — The Oracle JDBC thin driver (`ojdbc11.jar`) requires no OCI
client installation on the pgloader host; it connects directly over the network.
v4's JDBC architecture already handles driver loading at startup, so wiring in
the Oracle driver is the smallest of the three pieces.

**2. Casting rules** — Oracle's type system has well-known PostgreSQL
equivalents that we can encode as a static mapping, the same way MySQL and
MS SQL casting rules are implemented today:

| Oracle | PostgreSQL |
|---|---|
| `NUMBER(p, 0)` | `bigint` / `integer` / `numeric(p)` based on precision |
| `NUMBER(p, s)` | `numeric(p, s)` |
| `VARCHAR2(n)` / `NVARCHAR2(n)` | `text` |
| `DATE` | `timestamp` (Oracle DATE includes time) |
| `TIMESTAMP(n)` | `timestamptz(n)` |
| `CLOB` / `NCLOB` | `text` |
| `BLOB` / `RAW` | `bytea` |
| `XMLTYPE` | `xml` |

**3. Catalog queries** — This is the main effort.  The Oracle catalog uses
`ALL_TABLES`, `ALL_COLUMNS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`,
`ALL_CONSTRAINTS`, `ALL_CONS_COLUMNS`, and `ALL_SEQUENCES` — all accessible
to any user with standard `SELECT` privileges.  We need SQL queries against
these views that produce the same shape as pgloader's internal catalog
representation (tables, columns with types, primary keys, indexes, foreign
keys, sequences), plus the HugSQL-style query files and the Source protocol
implementation wired to them.

A connection string example:

```sql
LOAD DATABASE
     FROM oracle://user:pass@host:1521/ORCL
     INTO postgresql://user:pass@pghost/db
WITH create tables, create indexes, reset sequences, foreign keys;
```

Sequences, primary keys, unique constraints, check constraints, and foreign
keys all map cleanly.  Stored procedures, triggers, and packages are out of
scope — those require manual porting and are not automatable in general.

This is the largest single feature on the wish list and the one with the
broadest commercial interest.  A well-scoped v4-only implementation (Oracle
source, PostgreSQL target) is the right starting point.

---

If you'd like to see this feature prioritised, sponsoring its development is
possible through [The Art of PostgreSQL OSS sponsorship
program](https://oss.theartofpostgresql.com). Sponsored features get
dedicated implementation time and are tracked to completion; the design above
is already in place and ready to go.
