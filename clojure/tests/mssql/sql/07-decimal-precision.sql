-- #1615/#1619 decimal(28,13) precision preserved — value must not be truncated
SELECT big_decimal FROM public.type_edge_cases ORDER BY id;
