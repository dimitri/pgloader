-- Full integration test seed for the pgloader v4 Clojure rewrite.
-- Compatible with MySQL 8.0.16+ and MariaDB 10.2+.
-- Loaded into `pgloader_mysql_unit_full` on both MySQL and MariaDB.

CREATE DATABASE IF NOT EXISTS pgloader_mysql_unit_full;
USE pgloader_mysql_unit_full;

-- #1629: datetime/timestamp precision modifier preserved (datetime(6) → timestamptz(6))
-- #1403: CURRENT_TIMESTAMP(6) default stripped to bare keyword
CREATE TABLE type_precision (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  ts6         DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  ts3         DATETIME(3)  DEFAULT NULL,
  t3          TIME(3)      DEFAULT NULL,
  plain_ts    DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO type_precision (ts3, t3) VALUES
  ('2024-06-01 12:34:56.123', '01:02:03.456'),
  ('2024-12-31 23:59:59.999', '23:59:59.999');

-- #1280: bit(N) DEFAULT b'0' passes through as bit literal
-- #1403: CURRENT_TIMESTAMP default is lowercased keyword (not quoted string)
CREATE TABLE bit_defaults (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  flags       BIT(8)       NOT NULL DEFAULT b'00000000',
  single_bit  BIT(1)       NOT NULL DEFAULT b'0',
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO bit_defaults (flags, single_bit) VALUES
  (b'10000001', b'1'),
  (b'00000000', b'0');

-- #1645: CHECK constraints.
-- MySQL 8.0.16+ enforces these and exposes them in information_schema.CHECK_CONSTRAINTS
-- with backtick-quoted identifiers.  MariaDB 10.2+ stores them but surfaces them
-- differently — pgloader reads an empty result from information_schema on MariaDB,
-- so the expected output for this test differs between the two (see
-- expected/04-check-constraints.mariadb.out).
CREATE TABLE salary_check (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  salary     DECIMAL(10,2) NOT NULL,
  age        INT NOT NULL,
  CONSTRAINT chk_salary_positive CHECK (salary > 0),
  CONSTRAINT chk_age_range       CHECK (age BETWEEN 18 AND 99)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO salary_check (name, salary, age) VALUES
  ('Alice', 75000.00, 30),
  ('Bob',   52000.50, 45);

-- #1560/#1196: ON UPDATE CURRENT_TIMESTAMP → PostgreSQL BEFORE UPDATE trigger
CREATE TABLE on_update_ts (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  payload     VARCHAR(200),
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO on_update_ts (payload) VALUES ('first'), ('second');

-- #1066: hex-to-bytea cast function
CREATE TABLE hex_binary (
  id      INT AUTO_INCREMENT PRIMARY KEY,
  label   VARCHAR(50),
  raw_hex VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO hex_binary (label, raw_hex) VALUES
  ('plain',    'DEADBEEF'),
  ('prefixed', '0xCAFEBABE'),
  ('empty',    '');

-- #1522: WITHOUT TYPE cast syntax
-- perms uses WITHOUT TYPE so the SET type is preserved as an enum array
-- and values are routed through set-to-enum-array.
CREATE TABLE set_column (
  id      INT AUTO_INCREMENT PRIMARY KEY,
  perms   SET('read','write','execute') NOT NULL DEFAULT 'read'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO set_column (perms) VALUES ('read'), ('read,write'), ('read,write,execute');

GRANT SELECT ON pgloader_mysql_unit_full.* TO 'pgloader'@'%';
FLUSH PRIVILEGES;
