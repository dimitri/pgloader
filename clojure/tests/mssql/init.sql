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

-- #1594: multi-column foreign key — verify all columns are preserved.
-- `composite_parent` has a two-column primary key; `composite_child` references
-- both columns via a single FK constraint.  Before the fix, only the last
-- column was kept, producing a broken one-column FK.
CREATE TABLE composite_parent (
    region_id   INT NOT NULL,
    product_id  INT NOT NULL,
    name        NVARCHAR(100),
    CONSTRAINT pk_composite_parent PRIMARY KEY (region_id, product_id)
);
GO

CREATE TABLE composite_child (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    region_id   INT NOT NULL,
    product_id  INT NOT NULL,
    qty         INT NOT NULL DEFAULT 1,
    CONSTRAINT fk_child_parent
        FOREIGN KEY (region_id, product_id)
        REFERENCES composite_parent(region_id, product_id)
);
GO

INSERT INTO composite_parent (region_id, product_id, name) VALUES
    (1, 10, N'Widget-A'),
    (1, 20, N'Widget-B'),
    (2, 10, N'Gadget-A');
GO

INSERT INTO composite_child (region_id, product_id, qty) VALUES
    (1, 10, 5),
    (1, 20, 3),
    (2, 10, 1);
GO

-- ============================================================
-- filtertest: separate database for #1578/#1603 regression.
--
-- The bug: MATERIALIZE VIEWS with no INCLUDING ONLY clause
-- caused view names to be pushed into the initially-nil
-- `including` filter, turning "fetch all tables' indexes" into
-- "fetch indexes only for the view" — silently dropping every
-- table index.
--
-- The load file connects to this database with no table filter
-- so that `including` starts as NIL.  After the load, we verify
-- that the table indexes and FK survived.
-- ============================================================
CREATE DATABASE filtertest;
GO
USE filtertest;
GO

CREATE SCHEMA filtered;
GO

CREATE TABLE filtered.products (
    id    INT IDENTITY(1,1) PRIMARY KEY,
    name  NVARCHAR(100) NOT NULL
);
GO

CREATE TABLE filtered.order_items (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    qty        INT NOT NULL DEFAULT 1,
    CONSTRAINT fk_items_products
        FOREIGN KEY (product_id) REFERENCES filtered.products(id)
);
GO

CREATE INDEX idx_items_product ON filtered.order_items(product_id);
GO

-- The view whose name must NOT contaminate the table index/FK lookup
CREATE VIEW filtered.items_view AS
    SELECT oi.id, p.name, oi.qty
    FROM   filtered.order_items oi
    JOIN   filtered.products    p  ON p.id = oi.product_id;
GO

INSERT INTO filtered.products (name) VALUES ('Widget'), ('Gadget');
GO

INSERT INTO filtered.order_items (product_id, qty) VALUES (1, 3), (2, 1), (1, 5);
GO
