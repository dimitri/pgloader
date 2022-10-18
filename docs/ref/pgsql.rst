.. _migrating_to_pgsql:

Postgres to Postgres
====================

This command instructs pgloader to load data from a database connection.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints. A default set of casting
rules are provided and might be overloaded and appended to by the command.

For a complete Postgres to Postgres solution including Change Data Capture
support with Logical Decoding, see `pgcopydb`__.

__ https://pgcopydb.readthedocs.io/

Using default settings
----------------------

Here is the simplest command line example, which might be all you need:

::

   $ pgloader pgsql://user@source/dbname pgsql://user@target/dbname

Using advanced options and a load command file
----------------------------------------------

Here's a short example of migrating a database from a PostgreSQL server to
another. The command would then be:

::

   $ pgloader pg.load


And the contents of the command file ``pg.load`` could be inspired from the
following:

::

   load database
     from pgsql://localhost/pgloader
     into pgsql://localhost/copy
  
   including only table names matching 'bits', ~/utilisateur/ in schema 'mysql'
   including only table names matching ~/geolocations/ in schema 'public'
   ;

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

PostgreSQL Database Source Specification: FROM
----------------------------------------------

Must be a connection URL pointing to a PostgreSQL database.

See the `SOURCE CONNECTION STRING` section above for details on how to write
the connection string. 

::

    pgsql://[user[:password]@][netloc][:port][/dbname][?option=value&...]


PostgreSQL Database Migration Options: WITH
-------------------------------------------

When loading from a `PostgreSQL` database, the following options are
supported, and the default *WITH* clause is: *no truncate*, *create schema*,
*create tables*, *include drop*, *create indexes*, *reset sequences*,
*foreign keys*, *downcase identifiers*, *uniquify index names*, *reindex*.

  - *include drop*

    When this option is listed, pgloader drops all the tables in the target
    PostgreSQL database whose names appear in the MySQL database. This
    option allows for using the same command several times in a row until
    you figure out all the options, starting automatically from a clean
    environment. Please note that `CASCADE` is used to ensure that tables
    are dropped even if there are foreign keys pointing to them. This is
    precisely what `include drop` is intended to do: drop all target tables
    and recreate them.

    Great care needs to be taken when using `include drop`, as it will
    cascade to *all* objects referencing the target tables, possibly
    including other tables that are not being loaded from the source DB.

  - *include no drop*

    When this option is listed, pgloader will not include any `DROP`
    statement when loading the data.

  - *truncate*

    When this option is listed, pgloader issue the `TRUNCATE` command
    against each PostgreSQL table just before loading data into it.

  - *no truncate*

    When this option is listed, pgloader issues no `TRUNCATE` command.

  - *disable triggers*

    When this option is listed, pgloader issues an `ALTER TABLE ... DISABLE
    TRIGGER ALL` command against the PostgreSQL target table before copying
    the data, then the command `ALTER TABLE ... ENABLE TRIGGER ALL` once the
    `COPY` is done.

    This option allows loading data into a pre-existing table ignoring the
    *foreign key constraints* and user defined triggers and may result in
    invalid *foreign key constraints* once the data is loaded. Use with
    care.

  - *create tables*

    When this option is listed, pgloader creates the table using the meta
    data found in the `MySQL` file, which must contain a list of fields with
    their data type. A standard data type conversion from DBF to PostgreSQL
    is done.

  - *create no tables*

    When this option is listed, pgloader skips the creation of table before
    loading data, target tables must then already exist.

    Also, when using *create no tables* pgloader fetches the metadata from
    the current target database and checks type casting, then will remove
    constraints and indexes prior to loading the data and install them back
    again once the loading is done.

  - *create indexes*

    When this option is listed, pgloader gets the definitions of all the
    indexes found in the MySQL database and create the same set of index
    definitions against the PostgreSQL database.

  - *create no indexes*

    When this option is listed, pgloader skips the creating indexes.
        
  - *drop indexes*
  
    When this option is listed, pgloader drops the indexes in the target
    database before loading the data, and creates them again at the end
    of the data copy.

  - *reindex*

    When this option is used, pgloader does both *drop indexes* before
    loading the data and *create indexes* once data is loaded.

  - *drop schema*
  
    When this option is listed, pgloader drops the target schema in the
    target PostgreSQL database before creating it again and all the objects
    it contains. The default behavior doesn't drop the target schemas.

  - *foreign keys*

    When this option is listed, pgloader gets the definitions of all the
    foreign keys found in the MySQL database and create the same set of
    foreign key definitions against the PostgreSQL database.

  - *no foreign keys*

    When this option is listed, pgloader skips creating foreign keys.

  - *reset sequences*

    When this option is listed, at the end of the data loading and after the
    indexes have all been created, pgloader resets all the PostgreSQL
    sequences created to the current maximum value of the column they are
    attached to.

    The options *schema only* and *data only* have no effects on this
    option.

  - *reset no sequences*

    When this option is listed, pgloader skips resetting sequences after the
    load.

    The options *schema only* and *data only* have no effects on this
    option.

  - *downcase identifiers*

    When this option is listed, pgloader converts all MySQL identifiers
    (table names, index names, column names) to *downcase*, except for
    PostgreSQL *reserved* keywords.

    The PostgreSQL *reserved* keywords are determined dynamically by using
    the system function `pg_get_keywords()`.

  - *quote identifiers*

    When this option is listed, pgloader quotes all MySQL identifiers so
    that their case is respected. Note that you will then have to do the
    same thing in your application code queries.

  - *schema only*

    When this option is listed pgloader refrains from migrating the data
    over. Note that the schema in this context includes the indexes when the
    option *create indexes* has been listed.

  - *data only*

    When this option is listed pgloader only issues the `COPY` statements,
    without doing any other processing.

  - *rows per range*
  
    How many rows are fetched per `SELECT` query when using *multiple
    readers per thread*, see above for details.

PostgreSQL Database Casting Rules
---------------------------------

The command *CAST* introduces user-defined casting rules.

The cast clause allows to specify custom casting rules, either to overload
the default casting rules or to amend them with special cases.

A casting rule is expected to follow one of the forms::

    type <type-name> [ <guard> ... ] to <pgsql-type-name> [ <option> ... ]
    column <table-name>.<column-name> [ <guards> ] to ...

It's possible for a *casting rule* to either match against a PostgreSQL data
type or against a given *column name* in a given *table name*. So it's
possible to migrate a table from a PostgreSQL database while changing and
`int` column to a `bigint` one, automatically.

The *casting rules* are applied in order, the first match prevents following
rules to be applied, and user defined rules are evaluated first.

The supported guards are:

  - *when default 'value'*

    The casting rule is only applied against MySQL columns of the source
    type that have given *value*, which must be a single-quoted or a
    double-quoted string.

  - *when typemod expression*

    The casting rule is only applied against MySQL columns of the source
    type that have a *typemod* value matching the given *typemod
    expression*. The *typemod* is separated into its *precision* and *scale*
    components.

    Example of a cast rule using a *typemod* guard::

      type char when (= precision 1) to char keep typemod

    This expression casts MySQL `char(1)` column to a PostgreSQL column of
    type `char(1)` while allowing for the general case `char(N)` will be
    converted by the default cast rule into a PostgreSQL type `varchar(N)`.

  - *with extra auto_increment*

    The casting rule is only applied against PostgreSQL attached to a
    sequence. This can be the result of doing that manually, using a
    `serial` or a `bigserial` data type, or an `identity` column.


The supported casting options are:

  - *drop default*, *keep default*

    When the option *drop default* is listed, pgloader drops any
    existing default expression in the MySQL database for columns of the
    source type from the `CREATE TABLE` statement it generates.

    The spelling *keep default* explicitly prevents that behaviour and
    can be used to overload the default casting rules.

  - *drop not null*, *keep not null*, *set not null*

    When the option *drop not null* is listed, pgloader drops any
    existing `NOT NULL` constraint associated with the given source
    MySQL datatype when it creates the tables in the PostgreSQL
    database.

    The spelling *keep not null* explicitly prevents that behaviour and
    can be used to overload the default casting rules.

    When the option *set not null* is listed, pgloader sets a `NOT NULL`
    constraint on the target column regardless whether it has been set
    in the source MySQL column.

  - *drop typemod*, *keep typemod*

    When the option *drop typemod* is listed, pgloader drops any
    existing *typemod* definition (e.g. *precision* and *scale*) from
    the datatype definition found in the MySQL columns of the source
    type when it created the tables in the PostgreSQL database.

    The spelling *keep typemod* explicitly prevents that behaviour and
    can be used to overload the default casting rules.

  - *using*

    This option takes as its single argument the name of a function to
    be found in the `pgloader.transforms` Common Lisp package. See above
    for details.

    It's possible to augment a default cast rule (such as one that
    applies against `ENUM` data type for example) with a *transformation
    function* by omitting entirely the `type` parts of the casting rule,
    as in the following example::

      column enumerate.foo using empty-string-to-null

PostgreSQL Views Support
------------------------

PostgreSQL views support allows pgloader to migrate view as if they were
base tables. This feature then allows for on-the-fly transformation of the
source schema, as the view definition is used rather than the base data.

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
returned by PostgreSQL rather than asking the user to specify the list.

PostgreSQL Partial Migration
----------------------------

INCLUDING ONLY TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expression* used
to limit the tables to migrate to a sublist.

Example::

  including only table names matching ~/film/, 'actor' in schema 'public'

EXCLUDING TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expression* used
to exclude table names from the migration. This filter only applies to the
result of the *INCLUDING* filter.

::
  
  excluding table names matching ~<ory> in schema 'public'

PostgreSQL Schema Transformations
---------------------------------
    
ALTER TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expressions*
that you want to target in the pgloader *ALTER TABLE* command. Available
actions are *SET SCHEMA*, *RENAME TO*, and *SET*::

    ALTER TABLE NAMES MATCHING ~/_list$/, 'sales_by_store', ~/sales_by/
      IN SCHEMA 'public'
     SET SCHEMA 'mv'
   
    ALTER TABLE NAMES MATCHING 'film' IN SCHEMA 'public' RENAME TO 'films'
    
    ALTER TABLE NAMES MATCHING ~/./ IN SCHEMA 'public' SET (fillfactor='40')
    
    ALTER TABLE NAMES MATCHING ~/./ IN SCHEMA 'public' SET TABLESPACE 'pg_default'

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

PostgreSQL Migration: limitations
---------------------------------

The only PostgreSQL objects supported at this time in pgloader are
extensions, schema, tables, indexes and constraints. Anything else is ignored.

  - Views are not migrated,

    Supporting views might require implementing a full SQL parser for the
    MySQL dialect with a porting engine to rewrite the SQL against
    PostgreSQL, including renaming functions and changing some constructs.

    While it's not theoretically impossible, don't hold your breath.

  - Triggers are not migrated

    The difficulty of doing so is not yet assessed.

  - Stored Procedures and Functions are not migrated.


Default PostgreSQL Casting Rules
--------------------------------

When migrating from PostgreSQL the following Casting Rules are provided::

  type int with extra auto_increment to serial
  type bigint with extra auto_increment to bigserial
  type "character varying" to text drop typemod


