-- params pkey-oid-list
--        fkey-oild-list
with pkeys(oid) as (
  values~{(~d)~^,~}
),
     knownfkeys(oid) as (
  values~{(~d)~^,~}
),
  pkdeps as (
  select pkeys.oid, pg_depend.objid
    from pg_depend
         join pkeys on pg_depend.refobjid = pkeys.oid
   where     classid = 'pg_catalog.pg_constraint'::regclass
         and refclassid = 'pg_catalog.pg_class'::regclass
)
 select n.nspname, c.relname, nf.nspname, cf.relname as frelname,
        r.oid as conoid, conname,
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef,
        pkdeps.oid as index_oid
   from pg_catalog.pg_constraint r
        JOIN pkdeps on r.oid = pkdeps.objid
        JOIN pg_class c on r.conrelid = c.oid
        JOIN pg_namespace n on c.relnamespace = n.oid
        JOIN pg_class cf on r.confrelid = cf.oid
        JOIN pg_namespace nf on cf.relnamespace = nf.oid
  where NOT EXISTS (select 1 from knownfkeys where oid = r.oid)
