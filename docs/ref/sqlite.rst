SQLite to Postgres
==================

This command instructs pgloader to load data from a SQLite file. Automatic
discovery of the schema is supported, including build of the indexes.

Using default settings
----------------------

Here is the simplest command line example, which might be all you need:

::

   $ pgloader sqlite:///path/to/file.db pgsql://pguser@pghost/dbname

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader db.load

Here's an example of the ``db.load`` contents then::

    load database
         from sqlite:///Users/dim/Downloads/lastfm_tags.db
         into postgresql:///tags

     with include drop, create tables, create indexes, reset sequences

      set work_mem to '16MB', maintenance_work_mem to '512 MB';

Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

SQLite Database Source Specification: FROM
------------------------------------------

Path or HTTP URL to a SQLite file, might be a `.zip` file.

SQLite Database Migration Options: WITH
---------------------------------------

When loading from a `SQLite` database, the following options are
supported:

When loading from a `SQLite` database, the following options are
supported, and the default *WITH* clause is: *no truncate*, *create
tables*, *include drop*, *create indexes*, *reset sequences*, *downcase
identifiers*, *encoding 'utf-8'*.

  - *include drop*

    When this option is listed, pgloader drops all the tables in the target
    PostgreSQL database whose names appear in the SQLite database. This
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

    This option allows loading data into a pre-existing table ignoring
    the *foreign key constraints* and user defined triggers and may
    result in invalid *foreign key constraints* once the data is loaded.
    Use with care.

  - *create tables*

    When this option is listed, pgloader creates the table using the meta
    data found in the `SQLite` file, which must contain a list of fields
    with their data type. A standard data type conversion from SQLite to
    PostgreSQL is done.

  - *create no tables*

    When this option is listed, pgloader skips the creation of table before
    loading data, target tables must then already exist.

    Also, when using *create no tables* pgloader fetches the metadata
    from the current target database and checks type casting, then will
    remove constraints and indexes prior to loading the data and install
    them back again once the loading is done.

  - *create indexes*

    When this option is listed, pgloader gets the definitions of all the
    indexes found in the SQLite database and create the same set of index
    definitions against the PostgreSQL database.

  - *create no indexes*

    When this option is listed, pgloader skips the creating indexes.

  - *drop indexes*
  
    When this option is listed, pgloader drops the indexes in the target
    database before loading the data, and creates them again at the end
    of the data copy.

  - *reset sequences*

    When this option is listed, at the end of the data loading and after
    the indexes have all been created, pgloader resets all the
    PostgreSQL sequences created to the current maximum value of the
    column they are attached to.

  - *reset no sequences*

    When this option is listed, pgloader skips resetting sequences after the
    load.

    The options *schema only* and *data only* have no effects on this
    option.

  - *schema only*

    When this option is listed pgloader will refrain from migrating the data
    over. Note that the schema in this context includes the indexes when the
    option *create indexes* has been listed.

  - *data only*

    When this option is listed pgloader only issues the `COPY` statements,
    without doing any other processing.

  - *encoding*

    This option allows to control which encoding to parse the SQLite text
    data with. Defaults to UTF-8.

SQLite Database Casting Rules
-----------------------------
    
The command *CAST* introduces user-defined casting rules.

The cast clause allows to specify custom casting rules, either to overload
the default casting rules or to amend them with special cases.

SQlite Database Partial Migrations
----------------------------------

INCLUDING ONLY TABLE NAMES LIKE
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table name patterns used to limit the
tables to migrate to a sublist.

Example::

 including only table names like 'Invoice%'

EXCLUDING TABLE NAMES LIKE
^^^^^^^^^^^^^^^^^^^^^^^^^^

Introduce a comma separated list of table name patterns used to exclude
table names from the migration. This filter only applies to the result of
the *INCLUDING* filter.

::
  
  excluding table names like 'appointments'

Default SQLite Casting Rules
----------------------------

When migrating from SQLite the following Casting Rules are provided:

Numbers::

  type tinyint to smallint using integer-to-string
  type integer to bigint   using integer-to-string

  type float to float   using float-to-string
  type real to real     using float-to-string
  type double to double precision     using float-to-string
  type numeric to numeric     using float-to-string
  type decimal to numeric     using float-to-string

Texts::

  type character  to text drop typemod
  type varchar    to text drop typemod
  type nvarchar   to text drop typemod
  type char       to text drop typemod
  type nchar      to text drop typemod
  type nvarchar   to text drop typemod
  type clob       to text drop typemod

Binary::

  type blob       to bytea

Date::

  type datetime    to timestamptz using sqlite-timestamp-to-timestamp
  type timestamp   to timestamptz using sqlite-timestamp-to-timestamp
  type timestamptz to timestamptz using sqlite-timestamp-to-timestamp


