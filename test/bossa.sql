---
--- We need to run some tests where we don't provide for the schema within
--- the pgloader command itself, so we prepare the schema "manually" here in
--- advance.
---

drop table if exists intf_derivatives, intf_stocks;

create table intf_stocks
  (
   ticker     text,
   quote_date date,
   open       numeric,
   high       numeric,
   low        numeric,
   close      numeric,
   volume     bigint
  );

create table intf_derivatives
  (
   ticker     text,
   quote_date date,
   open       numeric,
   high       numeric,
   low        numeric,
   close      numeric,
   volume     bigint,
   openint    bigint
  );
