DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT,
    email TEXT,
    country TEXT,
    age INTEGER
);
INSERT INTO customers VALUES
(101, 'Asha', 'asha@example.com', 'USA', 24),
(102, 'Ben', NULL, 'USA', 31),
(103, 'Cara', 'cara@example.com', 'UK', -4),
(104, 'David', 'david@example.com', 'UK', 42),
(105, 'Eva', 'eva@example.com', 'USA', 29);
