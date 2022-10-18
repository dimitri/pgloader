MS SQL to Postgres
==================

This command instructs pgloader to load data from a MS SQL database.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints.

Using default settings
----------------------

Here is the simplest command line example, which might be all you need:

::

   $ pgloader mssql://user@mshost/dbname pgsql://pguser@pghost/dbname

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader ms.load

And the contents of the command file ``ms.load`` could be inspired from the
following:

::

    load database
         from mssql://user@host/dbname
         into postgresql:///dbname

    including only table names like 'GlobalAccount' in schema 'dbo'

    set work_mem to '16MB', maintenance_work_mem to '512 MB'

    before load do $$ drop schema if exists dbo cascade; $$;

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

MS SQL Database Source Specification: FROM
------------------------------------------

Connection string to an existing MS SQL database server that listens and
welcome external TCP/IP connection. As pgloader currently piggybacks on the
FreeTDS driver, to change the port of the server please export the `TDSPORT`
environment variable.

MS SQL Database Migration Options: WITH
---------------------------------------

When loading from a `MS SQL` database, the same options as when loading a
`MYSQL` database are supported. Please refer to the MYSQL section. The
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

Please refer to the MS SQL CAST clause for details.

MS SQL Views Support
--------------------

MS SQL views support allows pgloader to migrate view as if they were base
tables. This feature then allows for on-the-fly transformation from MS SQL
to PostgreSQL, as the view definition is used rather than the base data.

MATERIALIZE VIEWS
^^^^^^^^^^^^^^^^^

This clause allows you to implement custom data processing at the data
source by providing a *view definition* against which pgloader will query
the data. It's not possible to just allow for plain `SQL` because we want to
know a lot about the exact data types of each column involved in the query
output.

This clause expect a comma separated list of view definitions, each one
being either the name of an existing view in your database or the following
expression::

  *name* `AS` `$$` *sql query* `$$`

The *name* and the *sql query* will be used in a `CREATE VIEW` statement at
the beginning of the data loading, and the resulting view will then be
dropped at the end of the data loading.

MATERIALIZE ALL VIEWS
^^^^^^^^^^^^^^^^^^^^^

Same behaviour as *MATERIALIZE VIEWS* using the dynamic list of views as
returned by MS SQL rather than asking the user to specify the list.

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

Introduce a comma separated list of table names or *regular expressions*
that you want to target in the pgloader *ALTER TABLE* command. Available
actions are *SET SCHEMA*, *RENAME TO*, and *SET*::

    ALTER TABLE NAMES MATCHING ~/_list$/, 'sales_by_store', ~/sales_by/
      IN SCHEMA 'dbo'
     SET SCHEMA 'mv'
   
    ALTER TABLE NAMES MATCHING 'film' IN SCHEMA 'dbo' RENAME TO 'films'
    
    ALTER TABLE NAMES MATCHING ~/./ IN SCHEMA 'dbo' SET (fillfactor='40')
    
    ALTER TABLE NAMES MATCHING ~/./ IN SCHEMA 'dbo' SET TABLESPACE 'tlbspc'

You can use as many such rules as you need. The list of tables to be
migrated is searched in pgloader memory against the *ALTER TABLE* matching
rules, and for each command pgloader stops at the first matching criteria
(regexp or string).

No *ALTER TABLE* command is sent to PostgreSQL, the modification happens at
the level of the pgloader in-memory representation of your source database
schema. In case of a name change, the mapping is kept and reused in the
*foreign key* and *index* support.

The *SET ()* action takes effect as a *WITH* clause for the `CREATE TABLE`
command that pgloader will run when it has to create a table.

The *SET TABLESPACE* action takes effect as a *TABLESPACE* clause for the
`CREATE TABLE` command that pgloader will run when it has to create a table.

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
  type nchar     to text drop typemod
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

