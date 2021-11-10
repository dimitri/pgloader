select toids.tnsp || '.' || toids.tnam , c.oid
  from sys_catalog.sys_class c
  join sys_catalog.sys_namespace n on n.oid = c.relnamespace
  join sysloader_toids toids on n.nspname::text = toids.tnsp
                           and c.relname::text = toids.tnam;
