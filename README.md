# PGLoader

pgloader is a data loading tool for PostgreSQL, using the `COPY` command.

Its main avantage over just using `COPY` or `\copy` and over using a
*Foreign Data Wrapper* is the transaction behaviour, where *pgloader* will
keep a separate file of rejected data and continue trying to `copy` good
data in your database.

The default PostgreSQL behaviour is transactional, which means that any
erroneous line in the input data (file or remote database) will stop the
bulk load for the whole table.

pgloader also implements data reformating, the main example of that being a
transformation from MySQL dates `0000-00-00` and `0000-00-00 00:00:00` to
PostgreSQL `NULL` value (because our calendar never had a *year zero*).

## INSTALL

pgloader is now a Common Lisp program, tested using the
[SBCL](http://sbcl.org/) and [CCL](http://ccl.clozure.com/) implementation
with [Quicklisp](http://www.quicklisp.org/beta/).

    apt-get install sbcl
	wget http://beta.quicklisp.org/quicklisp.lisp
	sbcl --load quicklisp.lisp
	* (quicklisp-quickstart:install)
	* (ql:add-to-init-file)

Now fetch pgloader sources into `~/quicklisp/local-projects/` so that you
can do:

    sbcl
	* (ql:quickload :pgloader)
	* (in-package :pgloader)
	* (stream-database-tables "weetix")

## Usage

## TODO

Some notes about what I intend to be working on next.

### internals & refactoring

  - review pgloader.pgsql:reformat-row date-columns arguments
  - review connection string handling for both PostgreSQL and MySQL
  - provide a better toplevel API
  - implement tests

### command & control

  - commands: `LOAD` and `INI` formats
  - compat with `SQL*Loader` format

### error management

  - error management with a local buffer (done)
  - error reporting (done)
  - add input line number to log file?

#### data input

  - import directly from MySQL, file based export/import (done)
  - import directly from MySQL streaming (done)
  - general CSV and Flexible Text source formats
  - fixed cols input data format
  - compressed input (gzip, other algos)
  - fetch data from S3

### data output

  - PostgreSQL COPY Text format output for any supported input

#### convenience

  - automatic creation of schema (from MySQL schema, or from CSV header)
  - pre-fetch some rows to guesstimate data types?

#### performances

  - some more parallelizing options
  - support for partitionning in pgloader itself

#### reformating

Data reformating is now going to have to happen in Common Lisp mostly, maybe
offer some other languages (cl-awk etc).

  - raw reformating, before rows are split
  - per column reformating
     - date (zero dates)
	 - integer and "" that should be NULL
  - user-defined columns (constants, functions of other rows)
  - column re-ordering

#### UI

  - add a web controler with pretty monitoring
  - launch new jobs from the web controler

#### crazy ideas

  - MySQL replication, reading from the binlog directly
  - plproxy (re-)sharding support
  - partitioning support
  - remote archiving support (with (delete returning *) insert into)
  
