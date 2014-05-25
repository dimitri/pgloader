# TODO

Some notes about what I intend to be working on next. You can sponsor any
and all of those ideas if you actually need them today, and you can also
sponsor new ideas not on the list yet.

## Data Formats

## MySQL Support

  - MATERIALIZE ALL VIEWS
  - Convert SQL dialect for SQL views
  - Triggers and Stored Procedures

## Other

### binary distribution

  - prepare an all-included binary for several platforms
  
### docs

  - host a proper website for the tool, with use cases and a tutorial

### error management

  - error management with a local buffer (done)
  - error reporting (done)
  - add input line number to log file?

### data output

  - PostgreSQL COPY Text format output for any supported input

#### convenience

  - automatic creation of schema even when loading from text files
  - pre-fetch some rows to guesstimate data types?

#### performances

  - some more parallelizing options
  - support for partitionning in pgloader itself

#### reformating

Data reformating is now going to have to happen in Common Lisp mostly, maybe
offer some other languages (cl-awk etc).

  - raw reformating, before rows are split

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

#### filtering

Add commands to pick different target tables depending on the data found
when reading from the source.

#### UI

  - add a web controler with pretty monitoring
  - launch new jobs from the web controler

#### crazy ideas

  - MySQL replication, reading from the binlog directly
  - plproxy (re-)sharding support
  - partitioning support
  - remote archiving support (with (delete returning *) insert into)
  
