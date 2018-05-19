select toids.tnsp || '.' || toids.tnam , c.oid
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid = c.relnamespace
  join pgloader_toids toids on n.nspname::text = toids.tnsp
                           and c.relname::text = toids.tnam;
