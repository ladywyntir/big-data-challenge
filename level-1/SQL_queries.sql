-- Select Queries to check the tables

-- shows 10 rows from each Table as specified
SELECT * FROM customers LIMIT 10;
SELECT * FROM products LIMIT 10;
SELECT * FROM review_id_table LIMIT 10;
SELECT * FROM vine_table LIMIT 10;


-- shows a count from each Table as specified
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM review_id_table;
SELECT COUNT(*) FROM vine_table;