-- #1758: BINARY(16) → uuid; BINARY(8) → bytea
SELECT uuid_col, data_col FROM mytest.binary_types ORDER BY id;
