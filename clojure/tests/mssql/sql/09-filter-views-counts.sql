-- #1578/#1603: row counts after filtered-views load
SELECT count(*) AS products    FROM filtered.products;
SELECT count(*) AS order_items FROM filtered.order_items;
SELECT count(*) AS items_view  FROM filtered.items_view;
