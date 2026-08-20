DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    row_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price REAL,
    status TEXT
);
INSERT INTO orders VALUES
(1, 1001, 101, 501, 2, 250.00, 'PAID'),
(2, 1002, 102, 502, 1, NULL, 'PAID'),
(3, 1003, 103, 503, -2, 125.00, 'UNKNOWN'),
(4, 1004, 104, 504, 3, 90.00, 'SHIPPED'),
(5, 1005, 105, 501, 2, 250.00, 'PAID'),
(6, 1005, 105, 501, 2, 250.00, 'PAID');
