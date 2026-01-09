-- Insert sample data
USE testdb;

-- Insert users
INSERT INTO users (username, email) VALUES
('john_doe', 'john.doe@example.com'),
('jane_smith', 'jane.smith@example.com'),
('bob_wilson', 'bob.wilson@example.com'),
('alice_johnson', 'alice.johnson@example.com'),
('charlie_brown', 'charlie.brown@example.com'),
('diana_prince', 'diana.prince@example.com'),
('eve_adams', 'eve.adams@example.com'),
('frank_miller', 'frank.miller@example.com'),
('grace_hopper', 'grace.hopper@example.com'),
('henry_ford', 'henry.ford@example.com');

-- Insert products
INSERT INTO products (name, description, price, stock_quantity, category) VALUES
('Laptop Pro 15', 'High-performance laptop with 16GB RAM', 1299.99, 50, 'Electronics'),
('Wireless Mouse', 'Ergonomic wireless mouse with USB receiver', 29.99, 200, 'Electronics'),
('Office Chair', 'Comfortable ergonomic office chair', 249.99, 30, 'Furniture'),
('Desk Lamp', 'LED desk lamp with adjustable brightness', 39.99, 100, 'Furniture'),
('USB-C Cable', '6ft USB-C charging cable', 12.99, 500, 'Accessories'),
('Mechanical Keyboard', 'RGB mechanical gaming keyboard', 89.99, 75, 'Electronics'),
('Monitor 27"', '4K UHD 27-inch monitor', 399.99, 40, 'Electronics'),
('Webcam HD', '1080p HD webcam with microphone', 69.99, 120, 'Electronics'),
('Laptop Stand', 'Aluminum laptop stand with cooling', 45.99, 80, 'Accessories'),
('Phone Stand', 'Adjustable phone holder for desk', 15.99, 150, 'Accessories'),
('Headphones', 'Noise-cancelling wireless headphones', 199.99, 60, 'Electronics'),
('Notebook Set', 'Pack of 5 premium notebooks', 24.99, 200, 'Stationery'),
('Pen Set', 'Premium ballpoint pen set (10 pcs)', 19.99, 300, 'Stationery'),
('Desk Organizer', 'Wooden desk organizer with compartments', 34.99, 90, 'Furniture'),
('Cable Management', 'Under-desk cable management tray', 22.99, 150, 'Accessories');

-- Insert orders
INSERT INTO orders (user_id, total_amount, status) VALUES
(1, 1329.98, 'delivered'),
(2, 249.99, 'shipped'),
(3, 489.97, 'processing'),
(1, 89.99, 'delivered'),
(4, 1699.97, 'delivered'),
(5, 52.98, 'pending'),
(2, 199.99, 'shipped'),
(6, 75.98, 'processing'),
(7, 1299.99, 'delivered'),
(8, 149.97, 'delivered'),
(3, 399.99, 'shipped'),
(9, 24.99, 'pending'),
(10, 444.98, 'processing'),
(4, 29.99, 'cancelled'),
(5, 89.99, 'delivered');

-- Insert order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
-- Order 1
(1, 1, 1, 1299.99),
(1, 2, 1, 29.99),
-- Order 2
(2, 3, 1, 249.99),
-- Order 3
(3, 4, 2, 39.99),
(3, 5, 10, 12.99),
(3, 10, 20, 15.99),
-- Order 4
(4, 6, 1, 89.99),
-- Order 5
(5, 1, 1, 1299.99),
(5, 7, 1, 399.99),
-- Order 6
(6, 4, 1, 39.99),
(6, 5, 1, 12.99),
-- Order 7
(7, 11, 1, 199.99),
-- Order 8
(8, 12, 2, 24.99),
(8, 13, 1, 19.99),
(8, 5, 1, 12.99),
-- Order 9
(9, 1, 1, 1299.99),
-- Order 10
(10, 8, 1, 69.99),
(10, 9, 1, 45.99),
(10, 14, 1, 34.99),
-- Order 11
(11, 7, 1, 399.99),
-- Order 12
(12, 12, 1, 24.99),
-- Order 13
(13, 7, 1, 399.99),
(13, 9, 1, 45.99),
-- Order 14
(14, 2, 1, 29.99),
-- Order 15
(15, 6, 1, 89.99);

-- Insert activity logs
INSERT INTO activity_logs (user_id, action, description, ip_address) VALUES
(1, 'LOGIN', 'User logged in successfully', '192.168.1.100'),
(1, 'VIEW_PRODUCT', 'Viewed product: Laptop Pro 15', '192.168.1.100'),
(1, 'ADD_TO_CART', 'Added Laptop Pro 15 to cart', '192.168.1.100'),
(1, 'CHECKOUT', 'Completed checkout', '192.168.1.100'),
(2, 'LOGIN', 'User logged in successfully', '192.168.1.101'),
(2, 'VIEW_PRODUCT', 'Viewed product: Office Chair', '192.168.1.101'),
(3, 'LOGIN', 'User logged in successfully', '192.168.1.102'),
(3, 'SEARCH', 'Searched for: desk accessories', '192.168.1.102'),
(4, 'LOGIN', 'User logged in successfully', '192.168.1.103'),
(5, 'REGISTER', 'New user registered', '192.168.1.104'),
(5, 'LOGIN', 'User logged in successfully', '192.168.1.104'),
(6, 'LOGIN', 'User logged in successfully', '192.168.1.105'),
(7, 'LOGIN', 'User logged in successfully', '192.168.1.106'),
(8, 'LOGIN', 'User logged in successfully', '192.168.1.107'),
(9, 'LOGIN', 'User logged in successfully', '192.168.1.108'),
(10, 'LOGIN', 'User logged in successfully', '192.168.1.109');

-- Create a test user with limited permissions for monitoring
CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED BY 'monitorpass';
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'monitor'@'%';
FLUSH PRIVILEGES;
