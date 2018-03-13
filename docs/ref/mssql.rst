Migrating a MS SQL Database to PostgreSQL
=========================================

This command instructs pgloader to load data from a MS SQL database.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints.

Here's an example::

    load database
         from mssql://user@host/dbname
         into postgresql:///dbname

    including only table names like 'GlobalAccount' in schema 'dbo'

    set work_mem to '16MB', maintenance_work_mem to '512 MB'

    before load do $$ drop schema if exists dbo cascade; $$;

The `mssql` command accepts the following clauses and options.

MS SQL Database Source Specification: FROM
------------------------------------------

Connection string to an existing MS SQL database server that listens and
welcome external TCP/IP connection. As pgloader currently piggybacks on the
FreeTDS driver, to change the port of the server please export the `TDSPORT`
environment variable.

MS SQL Database Migration Options: WITH
---------------------------------------

When loading from a `MS SQL` database, the same options as when loading a
`MySQL` database are supported. Please refer to the MySQL section. The
following options are added:

  - *create schemas*

    When this option is listed, pgloader creates the same schemas as found
    on the MS SQL instance. This is the default.

  - *create no schemas*

    When this option is listed, pgloader refrains from creating any schemas
    at all, you must then ensure that the target schema do exist.

MS SQL Database Casting Rules
-----------------------------
    
CAST
^^^^

The cast clause allows to specify custom casting rules, either to overload
the default casting rules or to amend them with special cases.

Please refer to the MySQL CAST clause for details.

MS SQL Partial Migration
------------------------


INCLUDING ONLY TABLE NAMES LIKE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table name patterns used to limit the
tables to migrate to a sublist. More than one such clause may be used, they
will be accumulated together.

Example::

  including only table names like 'GlobalAccount' in schema 'dbo'

EXCLUDING TABLE NAMES LIKE
^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table name patterns used to exclude
table names from the migration. This filter only applies to the result of
the *INCLUDING* filter.

::
   
   excluding table names matching 'LocalAccount' in schema 'dbo'

MS SQL Schema Transformations
-----------------------------

ALTER SCHEMA '...' RENAME TO '...'
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Allows to rename a schema on the flight, so that for instance the tables
found in the schema 'dbo' in your source database will get migrated into the
schema 'public' in the target database with this command::

  alter schema 'dbo' rename to 'public'

ALTER TABLE NAMES MATCHING ... IN SCHEMA '...'
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See the MySQL explanation for this clause above. It works the same in the
context of migrating from MS SQL, only with the added option to specify the
name of the schema where to find the definition of the target tables.

The matching is done in pgloader itself, with a Common Lisp regular
expression lib, so doesn't depend on the *LIKE* implementation of MS SQL,
nor on the lack of support for regular expressions in the engine.

MS SQL Driver setup and encoding
--------------------------------

pgloader is using the `FreeTDS` driver, and internally expects the data to
be sent in utf-8. To achieve that, you can configure the FreeTDS driver with
those defaults, in the file `~/.freetds.conf`::

    [global]
        tds version = 7.4
        client charset = UTF-8

Default MS SQL Casting Rules
----------------------------

When migrating from MS SQL the following Casting Rules are provided:

Numbers::

  type tinyint to smallint

  type float to float   using float-to-string
  type real to real     using float-to-string
  type double to double precision     using float-to-string
  type numeric to numeric     using float-to-string
  type decimal to numeric     using float-to-string
  type money to numeric     using float-to-string
  type smallmoney to numeric     using float-to-string

Texts::

  type char      to text drop typemod
  type nchat     to text drop typemod
  type varchar   to text drop typemod
  type nvarchar  to text drop typemod
  type xml       to text drop typemod

Binary::

  type binary    to bytea using byte-vector-to-bytea
  type varbinary to bytea using byte-vector-to-bytea

Date::

  type datetime    to timestamptz
  type datetime2   to timestamptz

Others::

  type bit to boolean
  type hierarchyid to bytea
  type geography to bytea
  type uniqueidentifier to uuid using sql-server-uniqueidentifier-to-uuid

