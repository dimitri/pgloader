-- params pkey-oid-list
--        fkey-oild-list
with pkeys(oid) as (
  values~{(~d)~^,~}
),
     knownfkeys(oid) as (
  values~{(~d)~^,~}
),
  pkdeps as (
  select pkeys.oid, sys_depend.objid
    from sys_depend
         join pkeys on sys_depend.refobjid = pkeys.oid
   where     classid = 'sys_catalog.sys_constraint'::regclass
         and refclassid = 'sys_catalog.sys_class'::regclass
)
 select n.nspname, c.relname, nf.nspname, cf.relname as frelname,
        r.oid as conoid, conname,
        sys_catalog.sys_get_constraintdef(r.oid, true) as condef,
        pkdeps.oid as index_oid
   from sys_catalog.sys_constraint r
        JOIN pkdeps on r.oid = pkdeps.objid
        JOIN sys_class c on r.conrelid = c.oid
        JOIN sys_namespace n on c.relnamespace = n.oid
        JOIN sys_class cf on r.confrelid = cf.oid
        JOIN sys_namespace nf on cf.relnamespace = nf.oid
  where NOT EXISTS (select 1 from knownfkeys where oid = r.oid)
