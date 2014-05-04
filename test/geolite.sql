create extension if not exists ip4r;

create schema if not exists geolite;

create table if not exists geolite.location
(
   locid      integer primary key,
   country    text,
   region     text,
   city       text,
   postalcode text,
   location   point,
   metrocode  text,
   areacode   text
);
       
create table if not exists geolite.blocks
(
   iprange    ip4r,
   locid      integer
);

create or replace function geolite.locate(ip ip4)
  returns geolite.location
 language sql
as $$
 select l.locid, country, region, city, postalcode, location, metrocode, areacode
   from geolite.location l join geolite.blocks b using(locid)
  where b.iprange >>= $1;
$$;

drop index if exists geolite.blocks_ip4r_idx;
truncate table geolite.blocks, geolite.location cascade;

