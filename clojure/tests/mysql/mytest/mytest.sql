-- Unified integration test seed for pgloader MySQL regression suite.
-- Combines: the legacy "my" test database, the mysql-unit-full feature tests,
-- and the former source/source2 databases — all in a single MySQL database.
--
-- Compatible with MySQL 8.0.16+ and MariaDB 10.2+.
-- Uses MySQL-version-gated conditional comments (/*!NNNNN ... */) where
-- a feature is unavailable on older servers or on MariaDB.
--
-- NOTE — qmynd authentication (pgloader v3 / CL):
--   MySQL 8 defaults to caching_sha2_password, which the qmynd library does
--   not support.  The docker-compose service starts MySQL with
--     --default-authentication-plugin=mysql_native_password
--   so the pgloader user created by MYSQL_USER is already compatible.
--   If you're connecting qmynd against a plain MySQL 8 instance (no flag),
--   run manually:
--     ALTER USER 'pgloader'@'%' IDENTIFIED WITH mysql_native_password BY 'pgloader';
--     FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS pgloader_mytest;

-- Disable strict mode for this session so zero-date values in legacy tables
-- (races.date DEFAULT '0000-00-00', events.created_at DEFAULT '0000-00-00 00:00:00')
-- are accepted without error.
SET SESSION sql_mode='';

USE pgloader_mytest;

-- ============================================================
-- Legacy "my" tables (originally in the pgloader database)
-- These cover edge cases accumulated from reported pgloader bugs.
-- ============================================================

CREATE TABLE `empty` (id integer AUTO_INCREMENT PRIMARY KEY);

-- races: basic table with date/time columns and a MyISAM engine
CREATE TABLE `races` (
  `raceId` int(11) NOT NULL AUTO_INCREMENT,
  `year`      int(11) NOT NULL DEFAULT 0,
  `round`     int(11) NOT NULL DEFAULT 0,
  `circuitId` int(11) NOT NULL DEFAULT 0,
  `name`      varchar(255) NOT NULL DEFAULT '',
  `date`      date NOT NULL DEFAULT '0000-00-00',
  `time`      time DEFAULT NULL,
  `url`       varchar(255) DEFAULT NULL,
  PRIMARY KEY (`raceId`),
  UNIQUE KEY `url` (`url`)
) ENGINE=MyISAM AUTO_INCREMENT=989 DEFAULT CHARSET=utf8;

-- utilisateurs: issue #650 — enum with unicode values, non-ASCII table name
CREATE TABLE `utilisateurs__Yvelines2013-06-28` (
  `statut`    enum('administrateur','odis','pilote','bureau','relais','stagiaire','membre','participant','contact') COLLATE utf8_unicode_ci NOT NULL,
  `anciennete` year(4) DEFAULT NULL,
  `sexe`      enum('H','F') COLLATE utf8_unicode_ci DEFAULT NULL,
  `categorie` enum('Exploitant agricole','Artisan, commerçant, chef d''entreprise','Cadre, profession intellectuelle supérieure','Profession intermédiaire','Employé','Ouvrier','Retraité','Étudiant','Sans activité professionnelle') COLLATE utf8_unicode_ci DEFAULT NULL,
  `secteur`   enum('Secteur privé - Industrie','Secteur privé - Services','Secteur privé - Agriculture','Fonction publique d''Etat','Fonction publique hospitalière','Fonction publique territoriale','Secteur associatif et social') COLLATE utf8_unicode_ci DEFAULT NULL,
  `nombre`    enum('0','1','2','3 ?? 5','6 ?? 9','10 ?? 19','20 ?? 49','50 ?? 99','100 ?? 199','200 ?? 499','500 ?? 999','1 000 ?? 4 999','5 000 ?? 9 999','10 000 et plus') COLLATE utf8_unicode_ci DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- geom: geometry columns
CREATE TABLE `geom` (
  id integer AUTO_INCREMENT PRIMARY KEY,
  p  point,
  l  linestring
);

INSERT INTO `geom` (`p`, `l`) VALUES (
  ST_GeomFromText('POINT(1 1)'),
  ST_GeomFromText('LINESTRING(-87.87342467651445 45.79684462673078,-87.87170806274479 45.802110434248966,-87.84492888794016 45.79732335706963,-87.84046569213925 45.79349339919981,-87.83703246459994 45.795887153715135,-87.82261290893646 45.7954084110377,-87.82261290893646 45.781523084289475,-87.82879271850615 45.7796075953549,-87.84115233764729 45.779128712838755,-87.85110869751074 45.78008647375808,-87.83737578735439 45.782001946240946,-87.84527221069374 45.78343850741725)')
);

-- propertydecimal: issue #650 — decimal(18,0) / decimal(18,6) type mapping
CREATE TABLE propertydecimal (
  ID decimal(18,0) NOT NULL,
  propertyvalue decimal(18,6) DEFAULT NULL,
  PRIMARY KEY (ID)
);

-- base64: issue #650 — base64 encoded JSON in longtext column → jsonb
CREATE TABLE `base64` (
  id   varchar(255),
  data longtext
) COMMENT 'Test decoding base64 documents';

INSERT INTO `base64` (id, data) VALUES (
  '65de699d-b5cc-4e13-b507-c71adea31e53',
  'eyJrZXkiOiAidmFsdWUifQ=='
);

-- onupdate: ON UPDATE CURRENT_TIMESTAMP in the legacy "my" database
-- (distinct from on_update_ts below which tests the v4 trigger generation)
CREATE TABLE `onupdate` (
  `id`          int(11) NOT NULL AUTO_INCREMENT,
  `patient_id`  varchar(50) NOT NULL,
  `calc_date`   timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `patient_id` (`patient_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

-- funny_string: issue #661 — CREATE TABLE AS SELECT with GBK charset
CREATE TABLE funny_string AS SELECT char(41856 USING 'gbk') AS s;

-- pgloader_test_unsigned: issue #678 — UNSIGNED integer type mapping
CREATE TABLE pgloader_test_unsigned (
  id SMALLINT UNSIGNED,
  sm smallint,
  tu TINYINT UNSIGNED
);
INSERT INTO pgloader_test_unsigned (id) VALUES (65535);

-- bits: issue #684 — bit(1) → boolean
CREATE TABLE bits (
  id   integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
  bool bit(1)
);
INSERT INTO bits (bool) VALUES (0b00), (0b01);

-- domain_filter: issue #811 — binary/varbinary/json columns, ON UPDATE CURRENT_TIMESTAMP
CREATE TABLE `domain_filter` (
  `id`        binary(16) NOT NULL,
  `type`      varchar(50) NOT NULL,
  `value`     json DEFAULT NULL,
  `negated`   tinyint(1) NOT NULL DEFAULT '0',
  `report_id` varbinary(255) NOT NULL,
  `query_id`  varchar(255) NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `updated_by` varbinary(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `domain_filter_unq` (`report_id`, `query_id`, `type`),
  KEY `domain_filter` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii;

-- encryption_key_canary: issue #904 — binary(16) uuid, blob columns
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `encryption_key_canary` (
  `encrypted_value` blob,
  `nonce`           tinyblob,
  `uuid`            binary(16) NOT NULL,
  `salt`            tinyblob,
  PRIMARY KEY (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

LOCK TABLES `encryption_key_canary` WRITE;
/*!40000 ALTER TABLE `encryption_key_canary` DISABLE KEYS */;
INSERT INTO `encryption_key_canary` VALUES (
  0x1F36F183D7EE47C71453850B756945C16D9D711B2F0594E5D5E54D1EC94E081716AB8642AA60F84B50F69454D098122B7136A0DEB3AF200C2C5C7500BDFA0BD9689CCBF10A76972374882B304F7F15A227E815989FC87EEB72612396F569C662E72A2A7555E654605A3B83C1C753297832E52C5961E81EBC60DC43D929ABAB8CB14601DEFED121604CEB26210AB6D724,
  0x044AA707DF17021E55E9A1E4,
  0x88C2982F428A46B7B71B210618AE1658,
  0xAE7F18028E7984FB5630F7D23FB77999C6CA7CF5355EF0194F3F16521EA7EC503F566229ED8DC5EFBBE9C12BA491BDDC939FE60FA31FB9AF123B2B4D5B7A61FE
);
/*!40000 ALTER TABLE `encryption_key_canary` ENABLE KEYS */;
UNLOCK TABLES;

-- CamelCase: issue #703 — CamelCase table and column names
CREATE TABLE `CamelCase` (
  `validSizes` varchar(12)
);

-- countdata_template: issue #943 — bit(16) with binary default, French comment
CREATE TABLE `countdata_template` (
  `id`             int(11) NOT NULL AUTO_INCREMENT,
  `data`           int(11) DEFAULT NULL,
  `date_time`      datetime DEFAULT NULL,
  `gmt_offset`     smallint(6) NOT NULL DEFAULT '0' COMMENT 'Offset GMT en minute',
  `measurement_id` int(11) NOT NULL,
  `flags`          bit(16) NOT NULL DEFAULT b'0' COMMENT 'mot binaire : b1000=validé, b10000000=supprimé',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ak_countdata_idx` (`measurement_id`, `date_time`, `gmt_offset`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='données de comptage';

INSERT INTO `countdata_template` (`date_time`, `measurement_id`, `flags`) VALUES
  (now(), 1, b'1000'),
  (now(), 2, b'10000000');

-- uw_defined_meaning: issue #1102 — unsigned int columns
CREATE TABLE `uw_defined_meaning` (
  `defined_meaning_id` int(8) unsigned NOT NULL,
  `expression_id`      int(10) NOT NULL DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- fcm_batches: FULLTEXT index, mediumtext column
CREATE TABLE `fcm_batches` (
  `id`          int(10) unsigned NOT NULL AUTO_INCREMENT,
  `raw_payload` mediumtext COLLATE utf8_unicode_ci,
  `success`     int(10) unsigned NOT NULL DEFAULT '0',
  `failed`      int(10) unsigned NOT NULL DEFAULT '0',
  `modified`    int(10) unsigned NOT NULL DEFAULT '0',
  `created_at`  datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `fcm_batches_created_at_index` (`created_at`),
  FULLTEXT KEY `search` (`raw_payload`)
) ENGINE=InnoDB AUTO_INCREMENT=2501855 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

INSERT INTO `fcm_batches` (`raw_payload`, `created_at`) VALUES (
  'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
  now()
);

-- history: originally a separate file loaded into pgloader database
CREATE TABLE `history` (
  `hotel_id`      varchar(16) NOT NULL,
  `update_type`   varchar(255) NOT NULL,
  `code`          varchar(255) DEFAULT NULL,
  `affected_from` date DEFAULT NULL,
  `affected_to`   date DEFAULT NULL,
  `submit_time`   timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `submit_ip`     varchar(15) DEFAULT NULL,
  `submit_user`   varchar(255) DEFAULT NULL,
  `id`            bigint(20) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `history`
  ADD PRIMARY KEY (`id`) USING BTREE,
  ADD KEY `update_type` (`update_type`),
  ADD KEY `hotel_id` (`hotel_id`);

ALTER TABLE `history`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

-- ============================================================
-- Former "source" database tables
-- Previously a separate database; merged here for a single-database setup.
-- Note: the multi-database scenario (loading from source + source2 separately
-- into distinct PostgreSQL schemas) is no longer represented in this seed;
-- see the report in README or git log for the trade-offs.
-- ============================================================

-- users: basic users table with FK-referenced orders
CREATE TABLE users (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(100) NOT NULL,
  email      VARCHAR(255),
  age        INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_users_email ON users (email);

-- Expression index: MySQL 8.0.13+.
-- MariaDB ignores MySQL-version-gated conditional comments, so this index
-- is silently skipped there (MariaDB has a different expression-index syntax).
/*!80013 CREATE INDEX idx_users_email_lower ON users ((LOWER(email))) */;

INSERT INTO users (name, email, age) VALUES
  ('Alice',   'alice@example.com',   30),
  ('Bob',     'bob@example.com',     25),
  ('Charlie', 'charlie@example.com', 35);

CREATE TABLE orders (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  user_id    INT NOT NULL,
  amount     DECIMAL(10,2) NOT NULL,
  status     ENUM('pending', 'completed', 'cancelled') DEFAULT 'pending',
  ordered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_orders_user_id ON orders (user_id);

INSERT INTO orders (user_id, amount, status) VALUES
  (1, 99.99,  'completed'),
  (2, 49.50,  'pending'),
  (1, 149.99, 'completed');

ALTER TABLE orders ADD CONSTRAINT fk_orders_user
  FOREIGN KEY (user_id) REFERENCES users (id);

CREATE TABLE type_mapping_test (
  id        INT AUTO_INCREMENT PRIMARY KEY,
  flag      TINYINT(1)  DEFAULT 0 COMMENT 'boolean flag: tinyint(1) maps to pg boolean',
  small_val TINYINT(4)  DEFAULT 0 COMMENT 'regular tinyint maps to smallint',
  meta      JSON        DEFAULT NULL,
  boundary  GEOMETRY    DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO type_mapping_test (flag, small_val, meta) VALUES
  (1,  42,  '{"key": "value"}'),
  (0, 100,  '{"enabled": false}');

CREATE TABLE flags (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  label      VARCHAR(50) NOT NULL,
  is_active  TINYINT(1) NOT NULL DEFAULT 1,
  is_visible TINYINT(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO flags (label, is_active, is_visible) VALUES
  ('alpha', 1, 0),
  ('beta',  0, 1),
  ('gamma', 1, 1);

CREATE TABLE events (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(100),
  created_at DATETIME DEFAULT '0000-00-00 00:00:00',
  happened   DATETIME DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO events (name, created_at, happened) VALUES
  ('zero-date', '0000-00-00 00:00:00', '2024-01-15 10:00:00'),
  ('real-date', '2024-03-01 00:00:00', '2024-03-01 12:30:00');

CREATE TABLE measurements (
  id      INT AUTO_INCREMENT PRIMARY KEY,
  label   VARCHAR(50),
  reading SMALLINT UNSIGNED NOT NULL,
  score   TINYINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO measurements (label, reading, score) VALUES
  ('a', 60000,  100),
  ('b',     0,  -50),
  ('c', 32767,  127);

-- CamelCaseTable: camelCase identifier preservation (distinct from CamelCase above)
CREATE TABLE CamelCaseTable (
  MyId   INT AUTO_INCREMENT PRIMARY KEY,
  MyName VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO CamelCaseTable (MyName) VALUES ('one'), ('two');

-- things: formerly in the separate source2 database; merged here.
-- The original purpose was to verify multi-database loading; now a simple
-- extra table to confirm the merge didn't lose the row data.
CREATE TABLE things (
  id    INT AUTO_INCREMENT PRIMARY KEY,
  label VARCHAR(50)
);
INSERT INTO things (label) VALUES ('foo'), ('bar');

-- ============================================================
-- mysql-unit-full feature tables (#1629, #1403, #1280, #1645,
-- #1560/#1196, #1066, #1522)
-- ============================================================

-- #1629: datetime/timestamp precision modifier preserved (datetime(6) → timestamptz(6))
-- #1403: CURRENT_TIMESTAMP(6) default stripped to bare keyword
CREATE TABLE type_precision (
  id       INT AUTO_INCREMENT PRIMARY KEY,
  ts6      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  ts3      DATETIME(3) DEFAULT NULL,
  t3       TIME(3)     DEFAULT NULL,
  plain_ts DATETIME    DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO type_precision (ts3, t3) VALUES
  ('2024-06-01 12:34:56.123', '01:02:03.456'),
  ('2024-12-31 23:59:59.999', '23:59:59.999');

-- #1280: bit(N) DEFAULT b'0' passes through as bit literal
CREATE TABLE bit_defaults (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  flags      BIT(8) NOT NULL DEFAULT b'00000000',
  single_bit BIT(1) NOT NULL DEFAULT b'0',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO bit_defaults (flags, single_bit) VALUES
  (b'10000001', b'1'),
  (b'00000000', b'0');

-- #1645: CHECK constraints.
-- MySQL 8.0.16+ enforces these and exposes them in information_schema.CHECK_CONSTRAINTS.
-- MariaDB 10.2+ stores them but surfaces them differently — pgloader reads an empty
-- result from information_schema on MariaDB, so the expected output differs
-- between the two (see the .mariadb variant expected files).
CREATE TABLE salary_check (
  id     INT AUTO_INCREMENT PRIMARY KEY,
  name   VARCHAR(100) NOT NULL,
  salary DECIMAL(10,2) NOT NULL,
  age    INT NOT NULL,
  CONSTRAINT chk_salary_positive CHECK (salary > 0),
  CONSTRAINT chk_age_range       CHECK (age BETWEEN 18 AND 99)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO salary_check (name, salary, age) VALUES
  ('Alice', 75000.00, 30),
  ('Bob',   52000.50, 45);

-- #1560/#1196: ON UPDATE CURRENT_TIMESTAMP → PostgreSQL BEFORE UPDATE trigger
CREATE TABLE on_update_ts (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  payload    VARCHAR(200),
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
CREATE TABLE set_column (
  id    INT AUTO_INCREMENT PRIMARY KEY,
  perms SET('read','write','execute') NOT NULL DEFAULT 'read'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO set_column (perms) VALUES ('read'), ('read,write'), ('read,write,execute');

-- #1676: CAST ... when default "..." and not null — only matches NOT NULL columns.
-- This table has two datetime columns:
--   created_at: NOT NULL DEFAULT '0000-00-00 00:00:00' → matched by the cast rule
--   updated_at: nullable DEFAULT '0000-00-00 00:00:00' → NOT matched (null allowed)
-- After the load:
--   created_at should be a nullable timestamp (drop not null applied)
--              with the zero-date row converted to NULL
--   updated_at should remain typed as timestamp (the other cast rule, without
--              the not-null guard, applies) and the zero-date also becomes NULL
CREATE TABLE zero_dates_notnull (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  label      VARCHAR(50) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',
  updated_at DATETIME         DEFAULT '0000-00-00 00:00:00'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO zero_dates_notnull (label, created_at, updated_at) VALUES
  ('zero',    '0000-00-00 00:00:00', '0000-00-00 00:00:00'),
  ('real',    '2024-06-01 12:00:00', '2024-06-01 12:00:00'),
  ('null-upd','2024-06-02 09:00:00', NULL);

-- ============================================================
-- Grants
-- ============================================================
GRANT SELECT ON pgloader_mytest.* TO 'pgloader'@'%';
FLUSH PRIVILEGES;
