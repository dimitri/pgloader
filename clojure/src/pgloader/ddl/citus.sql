-- :name exec-create-reference-table :! :raw
-- :doc Create a Citus reference table; fqn is a quoted schema.table string e.g. "public"."t"
SELECT create_reference_table(:fqn)

-- :name exec-create-distributed-table :! :raw
-- :doc Create a Citus distributed table; fqn is quoted schema.table, col is the distribution column name
SELECT create_distributed_table(:fqn, :col)
