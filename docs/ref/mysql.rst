MySQL to Postgres
=================

This command instructs pgloader to load data from a database connection.
pgloader supports dynamically converting the schema of the source database
and the indexes building.

A default set of casting rules are provided and might be overloaded and
appended to by the command.

Using default settings
----------------------

Here is the simplest command line example, which might be all you need:

::

   $ pgloader mysql://myuser@myhost/dbname pgsql://pguser@pghost/dbname

Using advanced options and a load command file
----------------------------------------------

It might be that you want more flexibility than that and want to set
advanced options. Then the next example is using as many options as
possible, some of them even being defaults. Chances are you don't need that
complex a setup, don't copy and paste it, use it only as a reference!

The command then would be:

::

   $ pgloader my.load

And the contents of the command file ``my.load`` could be inspired from the
following:

::
   
    LOAD DATABASE
         FROM      mysql://root@localhost/sakila
         INTO postgresql://localhost:54393/sakila

     WITH include drop, create tables, create indexes, reset sequences,
          workers = 8, concurrency = 1,
          multiple readers per thread, rows per range = 50000

      SET PostgreSQL PARAMETERS
          maintenance_work_mem to '128MB',
          work_mem to '12MB',
          search_path to 'sakila, public, "$user"'
    
      SET MySQL PARAMETERS
          net_read_timeout  = '120',
          net_write_timeout = '120'

     CAST type bigint when (= precision 20) to bigserial drop typemod,
          type date drop not null drop default using zero-dates-to-null,
          -- type tinyint to boolean using tinyint-to-boolean,
          type year to integer

     MATERIALIZE VIEWS film_list, staff_list

     -- INCLUDING ONLY TABLE NAMES MATCHING ~/film/, 'actor'
     -- EXCLUDING TABLE NAMES MATCHING ~<ory>
     -- DECODING TABLE NAMES MATCHING ~/messed/, ~/encoding/ AS utf8
     -- ALTER TABLE NAMES MATCHING 'film' RENAME TO 'films'
     -- ALTER TABLE NAMES MATCHING ~/_list$/ SET SCHEMA 'mv'
     
     ALTER TABLE NAMES MATCHING ~/_list$/, 'sales_by_store', ~/sales_by/
      SET SCHEMA 'mv'
    
     ALTER TABLE NAMES MATCHING 'film' RENAME TO 'films'
     ALTER TABLE NAMES MATCHING ~/./ SET (fillfactor='40')
    
     ALTER SCHEMA 'sakila' RENAME TO 'pagila'

     BEFORE LOAD DO
       $$ create schema if not exists pagila; $$,
       $$ create schema if not exists mv;     $$,
       $$ alter database sakila set search_path to pagila, mv, public; $$;


Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

MySQL Database Source Specification: FROM
-----------------------------------------

Must be a connection URL pointing to a MySQL database.

If the connection URI contains a table name, then only this table is
migrated from MySQL to PostgreSQL.

See the `SOURCE CONNECTION STRING` section above for details on how to write
the connection string. The MySQL connection string accepts the same
parameter *sslmode* as the PostgreSQL connection string, but the *verify*
mode is not implemented (yet).

::

    mysql://[user[:password]@][netloc][:port][/dbname][?option=value&...]


MySQL connection strings support specific options:

  - ``useSSL``

    The same notation rules as found in the *Connection String* parts of the
    documentation apply, and we have a specific MySQL option: ``useSSL``.
    The value for ``useSSL`` can be either ``false`` or ``true``.

    If both ``sslmode`` and ``useSSL`` are used in the same connection
    string, pgloader behavior is undefined.
    
The MySQL connection string also accepts the *useSSL* parameter with values
being either *false* or *true*.

Environment variables described in
<http://dev.mysql.com/doc/refman/5.0/en/environment-variables.html> can be
used as default values too. If the user is not provided, then it defaults to
`USER` environment variable value. The password can be provided with the
environment variable `MYSQL_PWD`. The host can be provided with the
environment variable `MYSQL_HOST` and otherwise defaults to `localhost`. The
port can be provided with the environment variable `MYSQL_TCP_PORT` and
otherwise defaults to `3306`.

MySQL Database Migration Options: WITH
--------------------------------------

When loading from a `MySQL` database, the following options are supported,
and the default *WITH* clause is: *no truncate*, *create
tables*, *include drop*, *create indexes*, *reset sequences*, *foreign
keys*, *downcase identifiers*, *uniquify index names*.

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

  - *uniquify index names*, *preserve index names*

    MySQL index names are unique per-table whereas in PostgreSQL index names
    have to be unique per-schema. The default for pgloader is to change the
    index name by prefixing it with `idx_OID` where `OID` is the internal
    numeric identifier of the table the index is built against.

    In somes cases like when the DDL are entirely left to a framework it
    might be sensible for pgloader to refrain from handling index unique
    names, that is achieved by using the *preserve index names* option.

    The default is to *uniquify index names*.

    Even when using the option *preserve index names*, MySQL primary key
    indexes named "PRIMARY" will get their names uniquified. Failing to do
    so would prevent the primary keys to be created again in PostgreSQL
    where the index names must be unique per schema.

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

  - *single reader per thread*, *multiple readers per thread*
  
    The default is *single reader per thread* and it means that each
    MySQL table is read by a single thread as a whole, with a single
    `SELECT` statement using no `WHERE` clause.
    
    When using *multiple readers per thread* pgloader may be able to
    divide the reading work into several threads, as many as the
    *concurrency* setting, which needs to be greater than 1 for this
    option to kick be activated.
    
    For each source table, pgloader searches for a primary key over a
    single numeric column, or a multiple-column primary key index for
    which the first column is of a numeric data type (one of `integer`
    or `bigint`). When such an index exists, pgloader runs a query to
    find the *min* and *max* values on this column, and then split that
    range into many ranges containing a maximum of *rows per range*.
    
    When the range list we then obtain contains at least as many ranges
    than our concurrency setting, then we distribute those ranges to
    each reader thread.
    
    So when all the conditions are met, pgloader then starts as many
    reader thread as the *concurrency* setting, and each reader thread
    issues several queries with a `WHERE id >= x AND id < y`, where `y -
    x = rows per range` or less (for the last range, depending on the
    max value just obtained.
  
  - *rows per range*
  
    How many rows are fetched per `SELECT` query when using *multiple
    readers per thread*, see above for details.

  - *SET MySQL PARAMETERS*
  
    The *SET MySQL PARAMETERS* allows setting MySQL parameters using the
    MySQL `SET` command each time pgloader connects to it.

MySQL Database Casting Rules
----------------------------

The command *CAST* introduces user-defined casting rules.

The cast clause allows to specify custom casting rules, either to overload
the default casting rules or to amend them with special cases.

A casting rule is expected to follow one of the forms::

    type <mysql-type-name> [ <guard> ... ] to <pgsql-type-name> [ <option> ... ]
    column <table-name>.<column-name> [ <guards> ] to ...

It's possible for a *casting rule* to either match against a MySQL data type
or against a given *column name* in a given *table name*. That flexibility
allows to cope with cases where the type `tinyint` might have been used as a
`boolean` in some cases but as a `smallint` in others.

The *casting rules* are applied in order, the first match prevents following
rules to be applied, and user defined rules are evaluated first.

The supported guards are:

  - *when unsigned*

    The casting rule is only applied against MySQL columns of the source
    type that have the keyword *unsigned* in their data type definition.

    Example of a casting rule using a *unsigned* guard::
        
      type smallint when unsigned to integer drop typemod

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

    The casting rule is only applied against MySQL columns having the
    *extra* column `auto_increment` option set, so that it's possible to
    target e.g. `serial` rather than `integer`.

    The default matching behavior, when this option isn't set, is to match
    both columns with the extra definition and without.

    This means that if you want to implement a casting rule that target
    either `serial` or `integer` from a `smallint` definition depending on
    the *auto_increment* extra bit of information from MySQL, then you need
    to spell out two casting rules as following::

      type smallint  with extra auto_increment
        to serial drop typemod keep default keep not null,

      type smallint
        to integer drop typemod keep default keep not null

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

MySQL Views Support
-------------------

MySQL views support allows pgloader to migrate view as if they were base
tables. This feature then allows for on-the-fly transformation from MySQL to
PostgreSQL, as the view definition is used rather than the base data.

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
returned by MySQL rather than asking the user to specify the list.

MySQL Partial Migration
-----------------------

INCLUDING ONLY TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expression* used
to limit the tables to migrate to a sublist.

Example::

  including only table names matching ~/film/, 'actor'

EXCLUDING TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expression* used
to exclude table names from the migration. This filter only applies to the
result of the *INCLUDING* filter.

::
  
  excluding table names matching ~<ory>

MySQL Encoding Support
----------------------
      
DECODING TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expressions*
used to force the encoding to use when processing data from MySQL. If the
data encoding known to you is different from MySQL's idea about it, this is
the option to use.

::
  
  decoding table names matching ~/messed/, ~/encoding/ AS utf8

You can use as many such rules as you need, all with possibly different
encodings.

MySQL Schema Transformations
----------------------------
    
ALTER TABLE NAMES MATCHING
^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table names or *regular expressions*
that you want to target in the pgloader *ALTER TABLE* command. Available
actions are *SET SCHEMA*, *RENAME TO*, and *SET*::

    ALTER TABLE NAMES MATCHING ~/_list$/, 'sales_by_store', ~/sales_by/
     SET SCHEMA 'mv'
   
    ALTER TABLE NAMES MATCHING 'film' RENAME TO 'films'
    
    ALTER TABLE NAMES MATCHING ~/./ SET (fillfactor='40')

    ALTER TABLE NAMES MATCHING ~/./ SET TABLESPACE 'pg_default'

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

MySQL Migration: limitations
----------------------------

The `database` command currently only supports MySQL source database and has
the following limitations:

  - Views are not migrated,

    Supporting views might require implementing a full SQL parser for the
    MySQL dialect with a porting engine to rewrite the SQL against
    PostgreSQL, including renaming functions and changing some constructs.

    While it's not theoretically impossible, don't hold your breath.

  - Triggers are not migrated

    The difficulty of doing so is not yet assessed.

  - Of the geometric datatypes, only the `POINT` database has been covered.
    The other ones should be easy enough to implement now, it's just not
    done yet.

Default MySQL Casting Rules
---------------------------

When migrating from MySQL the following Casting Rules are provided:

Numbers::

  type int with extra auto_increment to serial when (< precision 10)
  type int with extra auto_increment to bigserial when (<= 10 precision)
  type int to int       when  (< precision 10)
  type int to bigint    when  (<= 10 precision)
  type tinyint   with extra auto_increment to serial
  type smallint  with extra auto_increment to serial
  type mediumint with extra auto_increment to serial
  type bigint    with extra auto_increment to bigserial

  type tinyint to boolean when (= 1 precision) using tinyint-to-boolean

  type bit when (= 1 precision) to boolean drop typemod using bits-to-boolean
  type bit to bit drop typemod using bits-to-hex-bitstring

  type bigint when signed to bigint drop typemod
  type bigint when (< 19 precision) to numeric drop typemod

  type tinyint when unsigned to smallint   drop typemod
  type smallint when unsigned to integer  drop typemod
  type mediumint when unsigned to integer  drop typemod
  type integer when unsigned to bigint    drop typemod
  
  type tinyint to smallint   drop typemod
  type smallint to smallint  drop typemod
  type mediumint to integer  drop typemod
  type integer to integer    drop typemod
  type bigint to bigint      drop typemod

  type float to float        drop typemod
  type double to double precision drop typemod

  type numeric to numeric keep typemod
  type decimal to decimal keep typemod

Texts::

  type char       to char keep typemod using remove-null-characters
  type varchar    to varchar keep typemod using remove-null-characters
  type tinytext   to text using remove-null-characters
  type text       to text using remove-null-characters
  type mediumtext to text using remove-null-characters
  type longtext   to text using remove-null-characters

Binary::

  type binary     to bytea using byte-vector-to-bytea
  type varbinary  to bytea using byte-vector-to-bytea
  type tinyblob   to bytea using byte-vector-to-bytea
  type blob       to bytea using byte-vector-to-bytea
  type mediumblob to bytea using byte-vector-to-bytea
  type longblob   to bytea using byte-vector-to-bytea

Date::
  
  type datetime when default "0000-00-00 00:00:00" and not null
    to timestamptz drop not null drop default
	using zero-dates-to-null

  type datetime when default "0000-00-00 00:00:00"
    to timestamptz drop default
	using zero-dates-to-null

  type datetime with extra on update current timestamp when not null
    to timestamptz drop not null drop default
       using zero-dates-to-null

  type datetime with extra on update current timestamp
    to timestamptz drop default
       using zero-dates-to-null

  type timestamp when default "0000-00-00 00:00:00" and not null
    to timestamptz drop not null drop default
	using zero-dates-to-null

  type timestamp when default "0000-00-00 00:00:00"
    to timestamptz drop default
	using zero-dates-to-null

  type date when default "0000-00-00" to date drop default
	using zero-dates-to-null

  type date to date
  type datetime to timestamptz
  type timestamp to timestamptz
  type year to integer drop typemod

Geometric::

  type geometry   to point using convert-mysql-point
  type point      to point using convert-mysql-point
  type linestring to path using convert-mysql-linestring

Enum types are declared inline in MySQL and separately with a `CREATE TYPE`
command in PostgreSQL, so each column of Enum Type is converted to a type
named after the table and column names defined with the same labels in the
same order.

When the source type definition is not matched in the default casting rules
nor in the casting rules provided in the command, then the type name with
the typemod is used.

