CREATE DATABASE IF NOT EXISTS citus_test;
USE citus_test;

CREATE TABLE IF NOT EXISTS companies (
  id   INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS campaigns (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  company_id INT NOT NULL,
  name       VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO companies (name) VALUES ('Acme'), ('Globex'), ('Initech');
INSERT INTO campaigns (company_id, name)
  VALUES (1, 'Spring Sale'), (1, 'Summer Sale'),
         (2, 'Rebranding'), (3, 'Launch');

-- Phase 2: billers/receivable_accounts/splits for FK backfill testing
DROP TABLE IF EXISTS splits;
DROP TABLE IF EXISTS receivable_accounts;
DROP TABLE IF EXISTS billers;

CREATE TABLE billers (
  id   INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE receivable_accounts (
  id        INT AUTO_INCREMENT PRIMARY KEY,
  biller_id INT NOT NULL,
  code      VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE splits (
  id                    INT AUTO_INCREMENT PRIMARY KEY,
  receivable_account_id INT NOT NULL,
  amount                DECIMAL(10,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE receivable_accounts ADD FOREIGN KEY (biller_id) REFERENCES billers(id);
ALTER TABLE splits ADD FOREIGN KEY (receivable_account_id) REFERENCES receivable_accounts(id);

INSERT INTO billers (name) VALUES ('Acme'), ('Globex');
INSERT INTO receivable_accounts (biller_id, code) VALUES (1,'A1'),(1,'A2'),(2,'B1');
INSERT INTO splits (receivable_account_id, amount) VALUES (1,100.00),(2,200.00),(3,50.00);
