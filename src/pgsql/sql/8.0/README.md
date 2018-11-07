Redshift is a fork of PostgreSQL 8.0, and our catalog queries must then
target this old PostgreSQL version to work on Redshift. Parts of what we
would usually implement in SQL is implemented in pgloader code instead, in
order to support such an old PostgreSQL version.
