DBF
===

This command instructs pgloader to load data from a `DBF` file. A default
set of casting rules are provided and might be overloaded and appended to by
the command.

Using advanced options and a load command file
----------------------------------------------

Here's an example with a remote HTTP source and some user defined casting
rules. The command then would be:

::

   $ pgloader dbf.load

And the contents of the ``dbf.load`` file could be inspired from the following:

::

    LOAD DBF
        FROM http://www.insee.fr/fr/methodes/nomenclatures/cog/telechargement/2013/dbf/reg2013.dbf
        INTO postgresql://user@localhost/dbname
        WITH truncate, create table
        CAST column reg2013.region to integer,
             column reg2013.tncc to smallint;


Common Clauses
--------------

Please refer to :ref:`common_clauses` for documentation about common
clauses.

DBF Source Specification: FROM
------------------------------

Filename where to load the data from. This support local files, HTTP URLs
and zip files containing a single dbf file of the same name. Fetch such a
zip file from an HTTP address is of course supported.

DBF Loading Options: WITH
-------------------------

When loading from a `DBF` file, the following options are supported:

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

Default DB3 Casting Rules
-------------------------

When migrating from DB3 the following Casting Rules are provided::

  type C to text using db3-trim-string
  type M to text using db3-trim-string
  type N to numeric using db3-numeric-to-pgsql-integer
  type I to numeric using db3-numeric-to-pgsql-numeric
  type L to boolean using logical-to-boolean
  type D to date using db3-date-to-pgsql-date

