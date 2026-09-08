-- ============================================================
-- vacuum.sql
-- ADAS Databricks Demo Project
-- ============================================================


-- ------------------------------------------------------------
-- 1. Check table history
-- ------------------------------------------------------------

DESCRIBE HISTORY adas_catalog.dev.silver_adas_events;


-- ------------------------------------------------------------
-- 2. Check current table details
-- ------------------------------------------------------------

DESCRIBE DETAIL adas_catalog.dev.silver_adas_events;


-- ------------------------------------------------------------
-- 3. Dry Run
-- ------------------------------------------------------------
-- Shows files that could potentially be removed.
-- Does NOT delete anything.

VACUUM adas_catalog.dev.silver_adas_events
RETAIN 168 HOURS
DRY RUN;


-- ------------------------------------------------------------
-- 4. Actual VACUUM
-- ------------------------------------------------------------
-- 168 hours = 7 days

VACUUM adas_catalog.dev.silver_adas_events
RETAIN 168 HOURS;


-- ------------------------------------------------------------
-- 5. Vacuum Gold Fact Table
-- ------------------------------------------------------------

VACUUM adas_catalog.dev.fact_adas_event
RETAIN 168 HOURS;
