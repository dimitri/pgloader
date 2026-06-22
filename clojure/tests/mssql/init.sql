CREATE DATABASE testdb;
GO
USE testdb;
GO

CREATE TABLE customers (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    name        NVARCHAR(100) NOT NULL,
    email       NVARCHAR(200),
    created     DATETIME2 DEFAULT GETDATE(),
    guid        UNIQUEIDENTIFIER,
    score       FLOAT,
    balance     DECIMAL(10,2)
);
GO

CREATE TABLE orders (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    amount      DECIMAL(10,2),
    order_date  DATE,
    notes       NTEXT,
    metadata    XML,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers(id)
);
GO

-- #976/#1445: datetimeoffset, sql_variant; high-precision decimal (#1615, #1619)
CREATE TABLE type_edge_cases (
    id           INT IDENTITY(1,1) PRIMARY KEY,
    dto          DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
    big_decimal  DECIMAL(28,13) NOT NULL,
    variant_col  SQL_VARIANT
);
GO

-- #1163: integer column with '' default (SQL Server coerces to 0; PostgreSQL rejects it)
-- #1409: CONVERT(datetime,...) default that cannot be expressed in PostgreSQL
CREATE TABLE default_edge_cases (
    id            INT IDENTITY(1,1) PRIMARY KEY,
    int_empty_def INT DEFAULT '',
    dt_convert    DATETIME DEFAULT CONVERT(DATETIME, '1753-01-01', 0)
);
GO

-- Basic index (non-filtered)
CREATE INDEX idx_orders_customer ON orders(customer_id);
GO

-- Partial / filtered index: only rows with a non-NULL email
-- QUOTED_IDENTIFIER ON is required by SQL Server for filtered indexes
SET QUOTED_IDENTIFIER ON;
CREATE INDEX idx_customers_email ON customers(email) WHERE email IS NOT NULL;
GO

-- A simple view used to test MATERIALIZE ALL VIEWS
CREATE VIEW active_customers AS
    SELECT id, name, email FROM customers WHERE email IS NOT NULL;
GO

INSERT INTO customers (name, email, score, balance) VALUES
    ('Alice',   'alice@example.com', 9.5,  1000.00),
    ('Bob',     'bob@example.com',   7.2,   500.50),
    ('Charlie', NULL,                NULL,    0.00);
GO

INSERT INTO orders (customer_id, amount, order_date, notes) VALUES
    (1, 99.99,  '2024-01-15', N'First order'),
    (1, 149.00, '2024-02-20', NULL),
    (2, 49.50,  '2024-03-01', N'Express');
GO

INSERT INTO type_edge_cases (big_decimal) VALUES
    (-67723280.0928298500000),
    (19942031.0000000000000),
    (0.0000000000001);
GO

INSERT INTO default_edge_cases DEFAULT VALUES;
GO
