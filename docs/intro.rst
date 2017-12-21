Introduction
============

pgloader loads data from various sources into PostgreSQL. It can
transform the data it reads on the fly and submit raw SQL before and
after the loading.  It uses the `COPY` PostgreSQL protocol to stream
the data into the server, and manages errors by filling a pair of
*reject.dat* and *reject.log* files.

pgloader knows how to read data from different kind of sources:

  * Files
    * CSV
    * Fixed Format
    * DBF
  * Databases
    * SQLite
    * MySQL
    * MS SQL Server

The level of automation provided by pgloader depends on the data source
type. In the case of CSV and Fixed Format files, a full description of the
expected input properties must be given to pgloader. In the case of a
database, pgloader connects to the live service and knows how to fetch the
metadata it needs directly from it.

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

Commands
--------

pgloader implements its own *Command Language*, a DSL that allows to specify
every aspect of the data load and migration to implement. Some of the
features provided in the language are only available for a specific source
type.

Command Line
------------

The pgloader command line accepts those two variants::

    pgloader [<options>] [<command-file>]...
    pgloader [<options>] SOURCE TARGET

Either you have a *command-file* containing migration specifications in the
pgloader *Command Language*, or you can give a *Source* for the data and a
PostgreSQL database connection *Target* where to load the data into.
