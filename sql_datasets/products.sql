DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price REAL,
    stock INTEGER
);
INSERT INTO products VALUES
(501, 'Laptop', 'Electronics', 50000.00, 12),
(502, 'Mouse', 'Electronics', 800.00, 45),
(503, 'Keyboard', 'Electronics', NULL, 20),
(504, 'Chair', 'Furniture', 4500.00, -3);
