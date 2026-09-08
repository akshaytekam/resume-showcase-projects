-- Now we handle small-file optimization in the Gold/Silver Delta tables.

-- 1. Optimize Silver ADAS Events

OPTIMIZE adas_catalog.dev.silver_adas_events;


-- 2. Optimize Fact Table

OPTIMIZE adas_catalog.dev.fact_adas_event;


-- 3. Z-ORDER Fact Table

OPTIMIZE adas_catalog.dev.fact_adas_event
ZORDER BY (
    vehicle_key,
    event_ts
);


-- 4. Z-ORDER Silver Table

OPTIMIZE adas_catalog.dev.silver_adas_events
ZORDER BY (
    vehicle_id,
    event_ts
);
