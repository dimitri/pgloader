# TODO

Some notes about what I intend to be working on next. You can sponsor any
and all of those ideas if you actually need them today, and you can also
sponsor new ideas not on the list yet.

## New Features

### Filtering

Add commands to pick different target tables depending on the data found
when reading from the source.

## Data Formats

### CSV

  - see about schema discovery (column names and types)

### JSON

Propose to load JSON either in a "document" column, or to normalize it by
applying some advanced filtering.

Implement PostgreSQL JSON operators and functions in pgloader to help setup
the normalisation steps:
[PostgreSQL JSON Functions and Operators](http://www.postgresql.org/docs/9.3/interactive/functions-json.html).

### XML

Add an XML reader to load XML documents into the database as a column value,
and XSLT capabilities to normalize the XML contents into a proper relational
model.

### Other databases

Add support for full data and schema migrations for the following:

  - SQL Server
  - Sybase
  - Oracle

## User Interface, User Experience

### Improve parse error messages

WIP, see https://github.com/nikodemus/esrap/issues/26

### Graphical User Interface

Most probably a web based tool, with guidance to setup the migration, maybe
not even something very sophisticated, but making the simple cases way
simpler.

## Database support

### MySQL Support

  - Convert SQL dialect for SQL views
  - Triggers and Stored Procedures
  
### SQLite support

  - implement CAST rules support

## Compat

  - add parsing for SQL*Loader file format

## Other

### error management

  - add input line number to log file?

### data output

  - PostgreSQL COPY Text format output for any supported input

### performances

  - some more parallelizing options
  - support for partitioning in pgloader itself

### UI

  - add a web controller with pretty monitoring
  - launch new jobs from the web controller

### crazy ideas

  - MySQL replication, reading from the binlog directly
  - plproxy (re-)sharding support
  - partitioning support
  - remote archiving support (with (delete returning *) insert into)
  
