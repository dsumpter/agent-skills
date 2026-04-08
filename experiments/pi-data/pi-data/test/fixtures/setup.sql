CREATE TABLE customers (
  id INTEGER PRIMARY KEY,
  name VARCHAR,
  region VARCHAR,
  created_at DATE
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  customer_id INTEGER,
  product VARCHAR,
  amount DECIMAL(10,2),
  created_at TIMESTAMP
);

INSERT INTO customers VALUES
  (1, 'Acme Corp', 'North America', '2024-01-15'),
  (2, 'Globex', 'Europe', '2024-02-20'),
  (3, 'Initech', 'North America', '2024-03-10');

INSERT INTO orders VALUES
  (1, 1, 'Widget Pro', 1000.00, '2024-02-01 12:00:00'),
  (2, 2, 'Widget Basic', 150.00, '2024-02-15 08:30:00'),
  (3, 3, 'Widget Starter', 50.00, '2024-03-05 09:45:00');
