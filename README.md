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
    apt-get install libmysqlclient-dev
	wget http://beta.quicklisp.org/quicklisp.lisp
	sbcl --load quicklisp.lisp
	* (quicklisp-quickstart:install)
	* (ql:add-to-init-file)

Now fetch pgloader sources using `git clone` then you can use the #! script.
You might have to modify it because it's now hard coded to use
`/usr/local/bin/sbcl` and you probably want to change that part then:

    ./pgloader.lisp --help

Each time you run the `pgloader` command line, it will check that all its
dependencies are installed and compiled and if that's not the case fetch
them from the internet and prepare them (thanks to *Quicklisp*). So please
be patient while that happens and make sure we can actually connect and
download the dependencies.

## Usage

Use the `--file` parameter to give pgloader a command file to parse, it will start 

    ./pgloader.lisp -f <file.load>

## TODO

Some notes about what I intend to be working on next.

### binary distribution

  - prepare an all-included binary for several platforms
  
### internals & refactoring

  - review pgloader.pgsql:reformat-row date-columns arguments
  - review connection string handling for both PostgreSQL and MySQL
  - provide a better toplevel API
  - implement tests

### command & control

  - commands: `LOAD` and `INI` formats
  - compat with `SQL*Loader` format

Here's a quick spec of the `LOAD` grammar:

    LOAD FROM '/path/to/filename.txt'
	          stdin
			  http://url.to/some/file.txt
			  mysql://[user[:pass]@][host[:port]]/dbname
		 [ COMPRESSED WITH zip | bzip2 | gzip ]
	
	WITH workers = 2,
		 batch size = 25000,
		 batch split = 5,
         reject file = '/tmp/pgloader/<table-name>.dat'
		 log file = '/tmp/pgloader/pgloader.log',
		 log level = debug | info | notice | warning | error | critical,
		 truncate,
         fields [ optionally ] enclosed by '"',
         fields escaped by '"',
         fields terminated by '\t',
         lines terminated by '\r\n',
		 encoding = 'latin9',
		 drop table,
		 create table,
		 create indexes,
		 reset sequences
		 
	 SET guc-1 = 'value', guc-2 = 'value'
	 
	 PREPARE CLIENT WITH ( <lisp> )
	 PREPARE SERVER WITH ( <sql> )
	 
	INTO postgresql://[user[:pass]@][host[:port]]/dbname?table-name
	     [ WITH <options> SET <gucs> ]
         (
		   field-name data-type field-desc [ with column options ],
		   ...
		 )
    USING (expression field-name other-field-name) as column-name,
	      ...
    
    INTO table-name  [ WITH <options> SET <gucs> ]
		 (
		   *
		 )

    WHEN 

	 FINALLY ON CLIENT DO ( <lisp> )
	         ON SERVER DO ( <lisp> )
    
    < data here if loading from stdin >
			 
The accepted column options are:

	terminated by ':'
    nullif { blank | zero date }
	date format "DD-Month-YYYY"
	
And we need a database migration command syntax too:
	
    LOAD DATABASE FROM mysql://localhost:3306/dbname
                  INTO postgresql://localhost/db
	WITH drop tables,
		 create tables,
		 create indexes,
		 reset sequences,
         <options>
	 SET guc = 'value', ...
	CAST tablename.column to timestamptz drop default,
		 varchar to text,
		 int with extra auto_increment to bigserial,
		 datetime to timestamptz drop default,
		 date to date drop default;

### docs

  - write proper documentation
  - host a proper website for the tool, with use cases and a tutorial

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

### transformation and casts

  - experiment with perfs and inlining the transformation functions
  - add typemod expression to cast rules in the command language
  - add per-column support for cast rules in the system

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

Have a try at something approaching:

    WITH data AS (
		COPY FROM ...
		RETURNING x, y
	)
	SELECT foo(x), bar(y)
	  FROM data
	 WHERE ...

A part of that needs to happen client-side, another part server-side, and
the grammar has to make it clear what happens where. Maybe add a WHERE
clause to the `COPY` or `LOAD` grammar for the client.

#### UI

  - add a web controler with pretty monitoring
  - launch new jobs from the web controler

#### crazy ideas

  - MySQL replication, reading from the binlog directly
  - plproxy (re-)sharding support
  - partitioning support
  - remote archiving support (with (delete returning *) insert into)
  
