.. pgloader documentation master file, created by
   sphinx-quickstart on Tue Dec  5 19:23:32 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pgloader's documentation!
====================================

The `pgloader`__ project is an Open Source Software project. The development
happens at `https://github.com/dimitri/pgloader`__ and is public: everyone
is welcome to participate by opening issues, pull requests, giving feedback,
etc.

__ https://github.com/dimitri/pgloader
__ https://github.com/dimitri/pgloader

pgloader loads data from various sources into PostgreSQL. It can transform
the data it reads on the fly and submit raw SQL before and after the
loading. It uses the `COPY` PostgreSQL protocol to stream the data into the
server, and manages errors by filling a pair of *reject.dat* and
*reject.log* files.

Thanks to being able to load data directly from a database source, pgloader
also supports from migrations from other productions to PostgreSQL. In this
mode of operations, pgloader handles both the schema and data parts of the
migration, in a single unmanned command, allowing to implement **Continuous
Migration**.

Features Overview
=================

pgloader has two modes of operation: loading from files, migrating
databases. In both cases, pgloader uses the PostgreSQL COPY protocol which
implements a **streaming** to send data in a very efficient way.

Loading file content in PostgreSQL
----------------------------------

When loading from files, pgloader implements the following features:

Many source formats supported
    Support for a wide variety of file based formats are included in
    pgloader: the CSV family, fixed columns formats, dBase files (``db3``),
    and IBM IXF files.

    The SQLite database engine is accounted for in the next section:
    pgloader considers SQLite as a database source and implements schema
    discovery from SQLite catalogs.

On the fly data transformation
    Often enough the data as read from a CSV file (or another format) needs
    some tweaking and clean-up before being sent to PostgreSQL.

    For instance in the `geolite
    <https://github.com/dimitri/pgloader/blob/master/test/archive.load>`_
    example we can see that integer values are being rewritten as IP address
    ranges, allowing to target an ``ip4r`` column directly.

Full Field projections
    pgloader supports loading data into less fields than found on file, or
    more, doing some computation on the data read before sending it to
    PostgreSQL.
    
Reading files from an archive
    Archive formats *zip*, *tar*, and *gzip* are supported by pgloader: the
    archive is extracted in a temporary directly and expanded files are then
    loaded.
    
HTTP(S) support
    pgloader knows how to download a source file or a source archive using
    HTTP directly. It might be better to use ``curl -O- http://... |
    pgloader`` and read the data from *standard input*, then allowing for
    streaming of the data from its source down to PostgreSQL.
    
Target schema discovery
    When loading in an existing table, pgloader takes into account the
    existing columns and may automatically guess the CSV format for you.
  
On error stop / On error resume next
    In some cases the source data is so damaged as to be impossible to
    migrate in full, and when loading from a file then the default for
    pgloader is to use ``on error resume next`` option, where the rows
    rejected by PostgreSQL are saved away and the migration continues with
    the other rows.

    In other cases loading only a part of the input data might not be a
    great idea, and in such cases it's possible to use the ``on error stop``
    option.

Pre/Post SQL commands
    This feature allows pgloader commands to include SQL commands to run
    before and after loading a file. It might be about creating a table
    first, then loading the data into it, and then doing more processing
    on-top of the data (implementing an *ELT* pipeline then), or creating
    specific indexes as soon as the data has been made ready.
    
One-command migration to PostgreSQL
-----------------------------------
  
When migrating a full database in a single command, pgloader implements the
following features:

One-command migration
    The whole migration is started with a single command line and then runs
    unattended. pgloader is meant to be integrated in a fully automated
    tooling that you can repeat as many times as needed.

Schema discovery
    The source database is introspected using its SQL catalogs to get the
    list of tables, attributes (with data types, default values, not null
    constraints, etc), primary key constraints, foreign key constraints,
    indexes, comments, etc. This feeds an internal database catalog of all
    the objects to migrate from the source database to the target database.

User defined casting rules
    Some source database have ideas about their data types that might not be
    compatible with PostgreSQL implementaion of equivalent data types.

    For instance, SQLite since version 3 has a `Dynamic Type System
    <https://www.sqlite.org/datatype3.html>`_ which of course isn't
    compatible with the idea of a `Relation
    <https://en.wikipedia.org/wiki/Relation_(database)>`_. Or MySQL accepts
    datetime for year zero, which doesn't exists in our calendar, and
    doesn't have a boolean data type.

    When migrating from another source database technology to PostgreSQL,
    data type casting choices must be made. pgloader implements solid
    defaults that you can rely upon, and a facility for **user defined data
    type casting rules** for specific cases. The idea is to allow users to
    specify the how the migration should be done, in order for it to be
    repeatable and included in a *Continuous Migration* process.

On the fly data transformations
    The user defined casting rules come with on the fly rewrite of the data.
    For instance zero dates (it's not just the year, MySQL accepts
    ``0000-00-00`` as a valid datetime) are rewritten to NULL values by
    default.
    
Partial Migrations
    It is possible to include only a partial list of the source database
    tables in the migration, or to exclude some of the tables on the source
    database.

Schema only, Data only
    This is the **ORM compatibility** feature of pgloader, where it is
    possible to create the schema using your ORM and then have pgloader
    migrate the data targeting this already created schema.

    When doing this, it is possible for pgloader to *reindex* the target
    schema: before loading the data from the source database into PostgreSQL
    using COPY, pgloader DROPs the indexes and constraints, and reinstalls
    the exact same definitions of them once the data has been loaded.

    The reason for operating that way is of course data load performance.
    
Repeatable (DROP+CREATE)
    By default, pgloader issues DROP statements in the target PostgreSQL
    database before issuing any CREATE statement, so that you can repeat the
    migration as many times as necessary until migration specifications and
    rules are bug free.

    The schedule the data migration to run every night (or even more often!)
    for the whole duration of the code migration project. See the
    `Continuous Migration <https://pgloader.io/blog/continuous-migration/>`_
    methodology for more details about the approach.

On error stop / On error resume next
    The default behavior of pgloader when migrating from a database is 
    ``on error stop``. The idea is to let the user fix either the migration 
    specifications or the source data, and run the process again, until 
    it works.

    In some cases the source data is so damaged as to be impossible to
    migrate in full, and it might be necessary to then resort to the ``on
    error resume next`` option, where the rows rejected by PostgreSQL are
    saved away and the migration continues with the other rows.

Pre/Post SQL commands, Post-Schema SQL commands
    While pgloader takes care of rewriting the schema to PostgreSQL
    expectations, and even provides *user-defined data type casting rules*
    support to that end, sometimes it is necessary to add some specific SQL
    commands around the migration. It's of course supported right from
    pgloader itself, without having to script around it.
    
Online ALTER schema
    At times migrating to PostgreSQL is also a good opportunity to review
    and fix bad decisions that were made in the past, or simply that are not
    relevant to PostgreSQL.

    The pgloader command syntax allows to ALTER pgloader's internal
    representation of the target catalogs so that the target schema can be
    created a little different from the source one. Changes supported
    include target a different *schema* or *table* name.
    
Materialized Views, or schema rewrite on-the-fly
    In some cases the schema rewriting goes deeper than just renaming the
    SQL objects to being a full normalization exercise. Because PostgreSQL
    is great at running a normalized schema in production under most
    workloads.

    pgloader implements full flexibility in on-the-fly schema rewriting, by
    making it possible to migrate from a view definition. The view attribute
    list becomes a table definition in PostgreSQL, and the data is fetched
    by querying the view on the source system.

    A SQL view allows to implement both content filtering at the column
    level using the SELECT projection clause, and at the row level using the
    WHERE restriction clause. And backfilling from reference tables thanks
    to JOINs.
    
Distribute to Citus
    When migrating from PostgreSQL to Citus, a important part of the process
    consists of adjusting the schema to the distribution key. Read
    `Preparing Tables and Ingesting Data
    <https://docs.citusdata.com/en/v8.0/use_cases/multi_tenant.html>`_ in
    the Citus documentation for a complete example showing how to do that.

    When using pgloader it's possible to specify the distribution keys and
    reference tables and let pgloader take care of adjusting the table,
    indexes, primary keys and foreign key definitions all by itself.

Encoding Overrides
    MySQL doesn't actually enforce the encoding of the data in the database
    to match the encoding known in the metadata, defined at the database,
    table, or attribute level. Sometimes, it's necessary to override the
    metadata in order to make sense of the text, and pgloader makes it easy
    to do so.


Continuous Migration
--------------------

pgloader is meant to migrate a whole database in a single command line and
without any manual intervention. The goal is to be able to setup a
*Continuous Integration* environment as described in the `Project
Methodology <http://mysqltopgsql.com/project/>`_ document of the `MySQL to
PostgreSQL <http://mysqltopgsql.com/project/>`_ webpage.

  1. Setup your target PostgreSQL Architecture
  2. Fork a Continuous Integration environment that uses PostgreSQL
  3. Migrate the data over and over again every night, from production
  4. As soon as the CI is all green using PostgreSQL, schedule the D-Day
  5. Migrate without suprise and enjoy! 

In order to be able to follow this great methodology, you need tooling to
implement the third step in a fully automated way. That's pgloader.

.. toctree::
   :hidden:
   :caption: Getting Started

   intro
   quickstart
   tutorial/tutorial
   install
   bugreport

.. toctree::
   :hidden:
   :caption: Reference Manual

   pgloader
   command
   batches
   ref/transforms

.. toctree::
   :hidden:
   :caption: Manual for file formats

   ref/csv
   ref/fixed
   ref/copy
   ref/dbf
   ref/ixf
   ref/archive
   
.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Manual for Database Servers

   ref/mysql
   ref/sqlite
   ref/mssql
   ref/pgsql
   ref/pgsql-citus-target
   ref/pgsql-redshift
   

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
