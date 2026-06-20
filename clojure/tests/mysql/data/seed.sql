CREATE DATABASE IF NOT EXISTS source;
USE source;

CREATE TABLE users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(255),
  age INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_users_email ON users(email);
-- MySQL 8.0 functional/expression index: tests that v4 translates (lower(`email`)) correctly
CREATE INDEX idx_users_email_lower ON users((LOWER(email)));

INSERT INTO users (name, email, age) VALUES
  ('Alice', 'alice@example.com', 30),
  ('Bob', 'bob@example.com', 25),
  ('Charlie', 'charlie@example.com', 35);

CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  status ENUM('pending', 'completed', 'cancelled') DEFAULT 'pending',
  ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_orders_user_id ON orders(user_id);

INSERT INTO orders (user_id, amount, status) VALUES
  (1, 99.99, 'completed'),
  (2, 49.50, 'pending'),
  (1, 149.99, 'completed');

ALTER TABLE orders ADD CONSTRAINT fk_orders_user
  FOREIGN KEY (user_id) REFERENCES users(id);

CREATE TABLE type_mapping_test (
  id INT AUTO_INCREMENT PRIMARY KEY,
  flag TINYINT(1) DEFAULT 0 COMMENT 'boolean flag: tinyint(1) maps to pg boolean',
  small_val TINYINT(4) DEFAULT 0 COMMENT 'regular tinyint maps to smallint',
  meta JSON DEFAULT NULL,
  boundary GEOMETRY DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO type_mapping_test (flag, small_val, meta) VALUES
  (1, 42, '{"key": "value"}'),
  (0, 100, '{"enabled": false}');

CREATE TABLE flags (
  id INT AUTO_INCREMENT PRIMARY KEY,
  label VARCHAR(50) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  is_visible TINYINT(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO flags (label, is_active, is_visible) VALUES
  ('alpha', 1, 0),
  ('beta', 0, 1),
  ('gamma', 1, 1);

CREATE TABLE events (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100),
  created_at DATETIME DEFAULT '0000-00-00 00:00:00',
  happened DATETIME DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO events (name, created_at, happened) VALUES
  ('zero-date', '0000-00-00 00:00:00', '2024-01-15 10:00:00'),
  ('real-date', '2024-03-01 00:00:00', '2024-03-01 12:30:00');

CREATE TABLE measurements (
  id INT AUTO_INCREMENT PRIMARY KEY,
  label VARCHAR(50),
  reading SMALLINT UNSIGNED NOT NULL,
  score TINYINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO measurements (label, reading, score) VALUES
  ('a', 60000, 100),
  ('b', 0, -50),
  ('c', 32767, 127);

CREATE TABLE CamelCaseTable (
  MyId INT AUTO_INCREMENT PRIMARY KEY,
  MyName VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO CamelCaseTable (MyName) VALUES ('one'), ('two');

CREATE DATABASE IF NOT EXISTS source2;
USE source2;

CREATE TABLE things (id INT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(50));
INSERT INTO things (label) VALUES ('foo'), ('bar');

USE source;

-- Grant read access to the pgloader user for all source databases
GRANT SELECT ON source.*  TO 'pgloader'@'%';
GRANT SELECT ON source2.* TO 'pgloader'@'%';
GRANT SELECT ON sakila.*  TO 'pgloader'@'%';
GRANT SELECT ON f1db.*    TO 'pgloader'@'%';
GRANT SELECT ON pgloader.* TO 'pgloader'@'%';

-- Use mysql_native_password so CL pgloader (qmynd) can authenticate.
-- MySQL 8 defaults to caching_sha2_password which qmynd does not support.
ALTER USER 'pgloader'@'%' IDENTIFIED WITH mysql_native_password BY 'pgloader';
FLUSH PRIVILEGES;
