-- Citus multi-tenant tutorial schema (pre-distribution version)
-- https://docs.citusdata.com/en/v7.5/use_cases/multi_tenant.html
--
-- This is the ORIGINAL schema before Citus distribution is applied.
-- ads/clicks/impressions do NOT have a company_id column yet.
-- pgloader adds it and backfills via JOIN when distributing.

CREATE TABLE companies (
  id         bigserial PRIMARY KEY,
  name       text NOT NULL,
  image_url  text,
  created_at timestamp without time zone NOT NULL,
  updated_at timestamp without time zone NOT NULL
);

CREATE TABLE campaigns (
  id                    bigserial PRIMARY KEY,
  company_id            bigint REFERENCES companies (id),
  name                  text NOT NULL,
  cost_model            text NOT NULL,
  state                 text NOT NULL,
  monthly_budget        bigint,
  blacklisted_site_urls text[],
  created_at            timestamp without time zone NOT NULL,
  updated_at            timestamp without time zone NOT NULL
);

CREATE TABLE ads (
  id                bigserial PRIMARY KEY,
  campaign_id       bigint REFERENCES campaigns (id),
  name              text NOT NULL,
  image_url         text,
  target_url        text,
  impressions_count bigint DEFAULT 0,
  clicks_count      bigint DEFAULT 0,
  created_at        timestamp without time zone NOT NULL,
  updated_at        timestamp without time zone NOT NULL
);

CREATE TABLE clicks (
  id                 bigserial PRIMARY KEY,
  ad_id              bigint REFERENCES ads (id),
  clicked_at         timestamp without time zone NOT NULL,
  site_url           text NOT NULL,
  cost_per_click_usd numeric(20,10),
  user_ip            inet NOT NULL,
  user_data          jsonb NOT NULL
);

CREATE TABLE impressions (
  id                      bigserial PRIMARY KEY,
  ad_id                   bigint REFERENCES ads (id),
  seen_at                 timestamp without time zone NOT NULL,
  site_url                text NOT NULL,
  cost_per_impression_usd numeric(20,10),
  user_ip                 inet NOT NULL,
  user_data               jsonb NOT NULL
);

-- Sample data matching the FK chain: companies → campaigns → ads → clicks/impressions

INSERT INTO companies (id, name, image_url, created_at, updated_at) VALUES
(1, 'Acme Corp',   'https://example.com/acme.png',   '2017-06-13 16:41:52', '2017-06-13 16:41:52'),
(2, 'Globex Inc',  'https://example.com/globex.png',  '2017-06-13 16:42:09', '2017-06-13 16:42:09'),
(3, 'Initech LLC', 'https://example.com/initech.png', '2017-06-13 16:42:15', '2017-06-13 16:42:15');

SELECT setval('companies_id_seq', 3);

INSERT INTO campaigns (id, company_id, name, cost_model, state, monthly_budget, blacklisted_site_urls, created_at, updated_at) VALUES
(1, 1, 'Spring Sale',    'cost_per_click',      'running', 500,  NULL, '2017-06-13 16:41:52', '2017-06-13 16:41:52'),
(2, 1, 'Summer Sale',    'cost_per_impression', 'paused',  800,  NULL, '2017-06-13 16:41:53', '2017-06-13 16:41:53'),
(3, 2, 'Rebranding',     'cost_per_click',      'running', 1200, NULL, '2017-06-13 16:42:09', '2017-06-13 16:42:09'),
(4, 3, 'Product Launch', 'cost_per_impression', 'running', 2000, NULL, '2017-06-13 16:42:15', '2017-06-13 16:42:15');

SELECT setval('campaigns_id_seq', 4);

INSERT INTO ads (id, campaign_id, name, image_url, target_url, impressions_count, clicks_count, created_at, updated_at) VALUES
(1, 1, 'Spring Banner',  'https://example.com/s1.png', 'http://acme.com/spring',   10, 2, '2017-06-13 16:41:52', '2017-06-13 16:41:52'),
(2, 1, 'Spring Promo',   'https://example.com/s2.png', 'http://acme.com/promo',    20, 5, '2017-06-13 16:41:53', '2017-06-13 16:41:53'),
(3, 2, 'Summer Ad',      'https://example.com/su.png', 'http://acme.com/summer',    5, 1, '2017-06-13 16:41:54', '2017-06-13 16:41:54'),
(4, 3, 'Globex Rebrand', 'https://example.com/g1.png', 'http://globex.com/new',    30, 8, '2017-06-13 16:42:09', '2017-06-13 16:42:09'),
(5, 4, 'Launch Ad',      'https://example.com/l1.png', 'http://initech.com/launch', 0, 0, '2017-06-13 16:42:15', '2017-06-13 16:42:15');

SELECT setval('ads_id_seq', 5);

INSERT INTO clicks (id, ad_id, clicked_at, site_url, cost_per_click_usd, user_ip, user_data) VALUES
(1, 1, '2017-06-14 08:00:00', 'http://news.example.com',  0.05, '192.168.1.1', '{"location":"US","is_mobile":false}'),
(2, 1, '2017-06-14 09:00:00', 'http://blog.example.com',  0.05, '10.0.0.1',    '{"location":"CA","is_mobile":true}'),
(3, 2, '2017-06-14 10:00:00', 'http://news.example.com',  0.07, '172.16.0.1',  '{"location":"GB","is_mobile":false}'),
(4, 4, '2017-06-14 11:00:00', 'http://sports.example.com',0.08, '203.0.113.1', '{"location":"AU","is_mobile":true}');

SELECT setval('clicks_id_seq', 4);

INSERT INTO impressions (id, ad_id, seen_at, site_url, cost_per_impression_usd, user_ip, user_data) VALUES
(1, 1, '2017-06-14 08:00:00', 'http://news.example.com',   0.001, '192.168.1.1',  '{"location":"US","is_mobile":false}'),
(2, 2, '2017-06-14 08:30:00', 'http://blog.example.com',   0.001, '10.0.0.1',     '{"location":"CA","is_mobile":true}'),
(3, 3, '2017-06-14 09:00:00', 'http://travel.example.com', 0.002, '172.16.0.1',   '{"location":"GB","is_mobile":false}'),
(4, 4, '2017-06-14 09:30:00', 'http://sports.example.com', 0.002, '203.0.113.1',  '{"location":"AU","is_mobile":true}'),
(5, 5, '2017-06-14 10:00:00', 'http://tech.example.com',   0.001, '198.51.100.1', '{"location":"DE","is_mobile":false}');

SELECT setval('impressions_id_seq', 5);
