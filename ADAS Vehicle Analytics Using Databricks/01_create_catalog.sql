-- ----------------------------------------------------
-- 1. Create Catalog
-- ------------------------------------------------------------

CREATE CATALOG IF NOT EXISTS adas_catalog;


-- ------------------------------------------------------------
-- 2. Create Environment Schemas
-- ------------------------------------------------

CREATE SCHEMA IF NOT EXISTS adas_catalog.dev;

CREATE SCHEMA IF NOT EXISTS adas_catalog.test;

CREATE SCHEMA IF NOT EXISTS adas_catalog.prod;


-- ---------------------------------------------------------
-- 3. Create Volumes
-- ------------------------------------------------------------

CREATE VOLUME IF NOT EXISTS adas_catalog.dev.adas_volume;

CREATE VOLUME IF NOT EXISTS adas_catalog.test.adas_volume;

CREATE VOLUME IF NOT EXISTS adas_catalog.prod.adas_volume;
