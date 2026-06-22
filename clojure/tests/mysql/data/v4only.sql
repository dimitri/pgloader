-- V4-only test schema: features implemented in the Clojure rewrite that
-- have no equivalent in pgloader v3 or produce different output.
-- Loaded into the `pgloader_v4` database on MySQL 8.0.

CREATE DATABASE IF NOT EXISTS pgloader_v4;
USE pgloader_v4;

-- #1629: datetime/timestamp precision modifier preserved (datetime(6) → timestamptz(6))
-- #1403: CURRENT_TIMESTAMP(6) default stripped to current_timestamp
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

-- #1645: CHECK constraints (MySQL 8.0.16+)
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

-- #1560/#1196: ON UPDATE CURRENT_TIMESTAMP → PostgreSQL trigger
CREATE TABLE on_update_ts (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  payload     VARCHAR(200),
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO on_update_ts (payload) VALUES ('first'), ('second');

-- #1066: hex-to-bytea cast function
-- Store hex strings that should land in PostgreSQL as bytea \x...
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
-- The `status` column uses WITHOUT TYPE so the pgloader SET type is kept
-- as text[] in PostgreSQL, applying only the cast function (set-to-enum-array).
CREATE TABLE set_column (
  id      INT AUTO_INCREMENT PRIMARY KEY,
  perms   SET('read','write','execute') NOT NULL DEFAULT 'read'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO set_column (perms) VALUES ('read'), ('read,write'), ('read,write,execute');

-- Grant access to the pgloader user
GRANT SELECT ON pgloader_v4.* TO 'pgloader'@'%';
FLUSH PRIVILEGES;
