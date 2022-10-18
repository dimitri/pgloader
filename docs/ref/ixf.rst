IXF
===

This command instructs pgloader to load data from an IBM `IXF` file.

Using advanced options and a load command file
----------------------------------------------

The command then would be:

::

   $ pgloader ixf.load

And the contents of the ``ixf.load`` file could be inspired from the following:

::
   
    LOAD IXF
        FROM data/nsitra.test1.ixf
        INTO postgresql:///pgloader
      TARGET TABLE nsitra.test1
        WITH truncate, create table, timezone UTC

      BEFORE LOAD DO
       $$ create schema if not exists nsitra; $$,
       $$ drop table if exists nsitra.test1; $$;


Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

IXF Source Specification: FROM
------------------------------

Filename where to load the data from. This support local files, HTTP URLs
and zip files containing a single ixf file of the same name. Fetch such a
zip file from an HTTP address is of course supported.

IXF Loading Options: WITH
-------------------------

When loading from a `IXF` file, the following options are supported:

  - *truncate*

    When this option is listed, pgloader issues a `TRUNCATE` command against
    the PostgreSQL target table before reading the data file.

  - *disable triggers*

    When this option is listed, pgloader issues an `ALTER TABLE ... DISABLE
    TRIGGER ALL` command against the PostgreSQL target table before copying
    the data, then the command `ALTER TABLE ... ENABLE TRIGGER ALL` once the
    `COPY` is done.

    This option allows loading data into a pre-existing table ignoring the
    *foreign key constraints* and user defined triggers and may result in
    invalid *foreign key constraints* once the data is loaded. Use with
    care.

  - *create table*

    When this option is listed, pgloader creates the table using the meta
    data found in the `DBF` file, which must contain a list of fields with
    their data type. A standard data type conversion from DBF to PostgreSQL
    is done.

  - *table name*

    This options expects as its value the possibly qualified name of the
    table to create.

  - *timezone*

    This options allows to specify which timezone is used when parsing
    timestamps from an IXF file, and defaults to *UTC*. Expected values are
    either `UTC`, `GMT` or a single quoted location name such as
    `'Universal'` or `'Europe/Paris'`.

