PostgreSQL to Citus
===================

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

Citus Migration Example
^^^^^^^^^^^^^^^^^^^^^^^

With the migration command as above, pgloader adds the column ``company_id``
to the tables that have a direct or indirect foreign key reference to the
``companies`` table.

We run pgloader using the following command, where the file
`./test/citus/company.load
<https://github.com/dimitri/pgloader/blob/master/test/citus/company.load>`_
contains the pgloader command as shown above.

::
   
   $ pgloader --client-min-messages sql ./test/citus/company.load

The following SQL statements are all extracted from the log messages that
the pgloader command outputs. We are going to have a look at the
`impressions` table. It gets created with a new column `company_id` in the
first position, as follows:

::
   
   CREATE TABLE "public"."impressions" 
   (
     company_id                bigint,
     "id"                      bigserial,
     "ad_id"                   bigint default NULL,
     "seen_at"                 timestamp with time zone default NULL,
     "site_url"                text default NULL,
     "cost_per_impression_usd" numeric(20,10) default NULL,
     "user_ip"                 inet default NULL,
     "user_data"               jsonb default NULL
   );

The original schema for this table does not have the `company_id` column,
which means pgloader now needs to change the primary key definition, the
foreign keys constraints definitions from and to this table, and also to
*backfill* the `company_id` data to this table when doing the COPY phase of
the migration.

Then once the tables have been created, pgloader executes the following SQL
statements::

  SELECT create_distributed_table('"public"."companies"', 'id');
  SELECT create_distributed_table('"public"."campaigns"', 'company_id');
  SELECT create_distributed_table('"public"."ads"', 'company_id');
  SELECT create_distributed_table('"public"."clicks"', 'company_id');
  SELECT create_distributed_table('"public"."impressions"', 'company_id');

Then when copying the data from the source PostgreSQL database to the new
Citus tables, the new column (here ``company_id``) needs to be backfilled
from the source tables. Here's the SQL query that pgloader uses as a data
source for the ``ads`` table in our example:

::

  SELECT "campaigns".company_id::text, "ads".id::text, "ads".campaign_id::text,
         "ads".name::text, "ads".image_url::text, "ads".target_url::text,
         "ads".impressions_count::text, "ads".clicks_count::text,
         "ads".created_at::text, "ads".updated_at::text
         
    FROM       "public"."ads"
         JOIN "public"."campaigns"
           ON ads.campaign_id = campaigns.id    

The ``impressions`` table has an indirect foreign key reference to the
``company`` table, which is the table where the distribution key is
specified. pgloader will discover that itself from walking the PostgreSQL
catalogs, and you may also use the following specification in the pgloader
command to explicitely add the indirect dependency:

::
   
   distribute impressions using company_id from ads, campaigns

Given this schema, the SQL query used by pgloader to fetch the data for the
`impressions` table is the following, implementing online backfilling of the
data:
   
::
   
   SELECT "campaigns".company_id::text, "impressions".id::text,
          "impressions".ad_id::text, "impressions".seen_at::text,
          "impressions".site_url::text,
          "impressions".cost_per_impression_usd::text,
          "impressions".user_ip::text,
          "impressions".user_data::text

     FROM      "public"."impressions"

          JOIN "public"."ads"
            ON impressions.ad_id = ads.id

          JOIN "public"."campaigns"
            ON ads.campaign_id = campaigns.id

When the data copying is done, then pgloader also has to install the indexes
supporting the primary keys, and add the foreign key definitions to the
schema. Those definitions are not the same as in the source schema, because
of the adding of the distribution column to the table: we need to also add
the column to the primary key and the foreign key constraints.

Here's the commands issued by pgloader for the ``impressions`` table:

::
   
   CREATE UNIQUE INDEX "impressions_pkey"
       ON "public"."impressions" (company_id, id);

   ALTER TABLE "public"."impressions"
     ADD CONSTRAINT "impressions_ad_id_fkey"
        FOREIGN KEY(company_id,ad_id)
         REFERENCES "public"."ads"(company_id,id)

Given a single line of specification ``distribute companies using id`` then
pgloader implements all the necessary schema changes on the fly when
migrating to Citus, and also dynamically backfills the data.
         
Citus Migration: Limitations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The way pgloader implements *reset sequence* does not work with Citus at
this point, so sequences need to be taken care of separately at this point.
