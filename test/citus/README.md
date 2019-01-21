# Citus Multi-Tenant Automatic Distribution

In this test case we follow the following documentation:

  https://docs.citusdata.com/en/v7.5/use_cases/multi_tenant.html
  
We install the schema before Citus migration, and load the data without the
backfilling that is already done. For that we use pgloader to ignore the
company_id column in the tables that didn't have this column prior to the
Citus migration effort.

Then the following `company.load` file contains the pgloader command that
runs a full migration from PostgreSQL to Citus:

```
load database
   from pgsql:///hackathon
   into pgsql://localhost:9700/dim

   with include drop, reset no sequences

   distribute companies using id;
```

Tables are marked distributed, the company_id column is added where it's
needed, primary keys and foreign keys definitions are altered to the new
model, and finally the data is backfilled automatically in the target table
thanks to generating queries like the following:

~~~
SELECT "campaigns".company_id::text,
       "impressions".id::text,
       "impressions".ad_id::text,
       "impressions".seen_at::text,
       "impressions".site_url::text,
       "impressions".cost_per_impression_usd::text,
       "impressions".user_ip::text,
       "impressions".user_data::text
  FROM "public"."impressions"  
        JOIN "public"."ads" ON impressions.ad_id = ads.id
        JOIN "public"."campaigns" ON ads.campaign_id = campaigns.id
~~~
