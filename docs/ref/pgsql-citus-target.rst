Migrating a PostgreSQL Database to Citus
========================================

This command instructs pgloader to load data from a database connection.
Automatic discovery of the schema is supported, including build of the
indexes, primary and foreign keys constraints. A default set of casting
rules are provided and might be overloaded and appended to by the command.

Automatic distribution column backfilling is supported, either from commands
that specify what is the distribution column in every table, or only in the
main table, then relying on foreign key constraints to discover the other
distribution keys.

Here's a short example of migrating a database from a PostgreSQL server to
another:

::

   load database
   from pgsql:///hackathon
   into pgsql://localhost:9700/dim

   with include drop, reset no sequences

   cast column impressions.seen_at to "timestamp with time zone"

   distribute companies using id
   -- distribute campaigns using company_id
   -- distribute ads using company_id from campaigns
   -- distribute clicks using company_id from ads, campaigns
   -- distribute impressions using company_id from ads, campaigns
   ;

Everything works exactly the same way as when doing a PostgreSQL to
PostgreSQL migration, with the added fonctionality of this new `distribute`
command.

Distribute Command
^^^^^^^^^^^^^^^^^^

The distribute command syntax is as following::

  distribute <table name> using <column name>
  distribute <table name> using <column name> from <table> [, <table>, ...]
  distribute <table name> as reference table

When using the distribute command, the following steps are added to pgloader
operations when migrating the schema:

  - if the distribution column does not exist in the table, it is added as
    the first column of the table

  - if the distribution column does not exists in the primary key of the
    table, it is added as the first column of the primary of the table

  - all the foreign keys that point to the table are added the distribution
    key automatically too, including the source tables of the foreign key
    constraints
  
  - once the schema has been created on the target database, pgloader then
    issues Citus specific command `create_reference_table()
    <http://docs.citusdata.com/en/v8.0/develop/api_udf.html?highlight=create_reference_table#create-reference-table>`_
    and `create_distributed_table()
    <http://docs.citusdata.com/en/v8.0/develop/api_udf.html?highlight=create_reference_table#create-distributed-table>`_
    to make the tables distributed

Those operations are done in the schema section of pgloader, before the data
is loaded. When the data is loaded, the newly added columns need to be
backfilled from referenced data. pgloader knows how to do that by generating
a query like the following and importing the result set of such a query
rather than the raw data from the source table.

Citus Migration: Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The way pgloader implements *reset sequence* does not work with Citus at
this point, so sequences need to be taken care of separately at this point.
