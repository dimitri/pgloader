.. pgloader documentation master file, created by
   sphinx-quickstart on Tue Dec  5 19:23:32 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pgloader's documentation!
====================================

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

.. toctree::
   :maxdepth: 2
   :caption: Table Of Contents:

   intro
   tutorial/tutorial
   pgloader
   ref/csv
   ref/fixed
   ref/copy
   ref/dbf
   ref/ixf
   ref/archive
   ref/mysql
   ref/sqlite
   ref/mssql
   ref/pgsql
   ref/pgsql-citus-target
   ref/pgsql-redshift
   ref/transforms
   bugreport

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
