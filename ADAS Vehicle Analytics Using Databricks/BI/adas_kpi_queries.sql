-- ============================================================
-- 1. Total ADAS Events
-- ============================================================

SELECT
    COUNT(*) AS total_events
FROM adas_catalog.dev.fact_adas_event;


-- ============================================================
-- 2. Events by Severity
-- ============================================================

SELECT
    e.severity,
    COUNT(*) AS event_count
FROM adas_catalog.dev.fact_adas_event f
JOIN adas_catalog.dev.dim_event e
    ON f.event_key = e.event_key
GROUP BY e.severity
ORDER BY event_count DESC;


-- ============================================================
-- 3. Events by Sensor
-- ============================================================

SELECT
    s.sensor_type,
    COUNT(*) AS event_count
FROM adas_catalog.dev.fact_adas_event f
JOIN adas_catalog.dev.dim_sensor s
    ON f.sensor_key = s.sensor_key
GROUP BY s.sensor_type
ORDER BY event_count DESC;


-- ============================================================
-- 4. Events by Vehicle
-- ============================================================

SELECT
    v.vehicle_id,
    v.vehicle_model,
    v.oem,
    COUNT(*) AS event_count
FROM adas_catalog.dev.fact_adas_event f
JOIN adas_catalog.dev.dim_vehicle v
    ON f.vehicle_key = v.vehicle_key
GROUP BY
    v.vehicle_id,
    v.vehicle_model,
    v.oem
ORDER BY event_count DESC;


-- ============================================================
-- 5. Events by Event Type
-- ============================================================

SELECT
    e.event_type,
    COUNT(*) AS event_count
FROM adas_catalog.dev.fact_adas_event f
JOIN adas_catalog.dev.dim_event e
    ON f.event_key = e.event_key
GROUP BY e.event_type
ORDER BY event_count DESC;


-- ============================================================
-- 6. Average Vehicle Speed
-- ============================================================

SELECT
    AVG(speed_kmph) AS average_speed_kmph
FROM adas_catalog.dev.fact_adas_event;


-- ============================================================
-- 7. Average Confidence
-- ============================================================

SELECT
    AVG(confidence) AS average_confidence
FROM adas_catalog.dev.fact_adas_event;


-- ============================================================
-- 8. Daily Event Trend
-- ============================================================

SELECT
    d.event_date,
    COUNT(*) AS event_count
FROM adas_catalog.dev.fact_adas_event f
JOIN adas_catalog.dev.dim_date d
    ON f.date_key = d.date_key
GROUP BY d.event_date
ORDER BY d.event_date;


-- ============================================================
-- 9. High Severity Events
-- ============================================================

SELECT
    COUNT(*) AS high_severity_events
FROM adas_catalog.dev.fact_adas_event f
JOIN adas_catalog.dev.dim_event e
    ON f.event_key = e.event_key
WHERE e.severity = 'HIGH';


-- ============================================================
-- 10. ADAS KPI Summary
-- ============================================================

SELECT
    COUNT(*) AS total_events,

    COUNT(
        CASE
            WHEN e.severity = 'HIGH'
            THEN 1
        END
    ) AS high_severity_events,

    COUNT(
        CASE
            WHEN e.severity = 'MEDIUM'
            THEN 1
        END
    ) AS medium_severity_events,

    COUNT(
        CASE
            WHEN e.severity = 'LOW'
            THEN 1
        END
    ) AS low_severity_events,

    ROUND(
        AVG(f.speed_kmph),
        2
    ) AS avg_speed_kmph,

    ROUND(
        AVG(f.confidence),
        3
    ) AS avg_confidence

FROM adas_catalog.dev.fact_adas_event f

JOIN adas_catalog.dev.dim_event e
    ON f.event_key = e.event_key;
