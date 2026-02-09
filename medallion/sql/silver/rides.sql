DROP TABLE IF EXISTS silver.rides;
CREATE TABLE silver.rides (
    ride_id TEXT PRIMARY KEY,
    rider_id TEXT,
    driver_id TEXT,
    city TEXT,
    ride_status TEXT,
    request_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    ride_duration_min NUMERIC(6,2),
    distance_km NUMERIC(6,2),
    fare_amount NUMERIC(10,2)
);

INSERT INTO silver.rides
SELECT DISTINCT
    ride_id,
    rider_id,
    driver_id,
    INITCAP(city),
    CASE
        WHEN LOWER(ride_status) IN ('completed','complete') THEN 'COMPLETED'
        WHEN LOWER(ride_status) IN ('cancelled by rider','canceled by rider') THEN 'CANCELLED_BY_RIDER'
        WHEN LOWER(ride_status) IN ('cancelled by driver','canceled by driver') THEN 'CANCELLED_BY_DRIVER'
        ELSE 'UNKNOWN'
    END,
    TO_TIMESTAMP(request_time, 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(start_time,   'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(end_time,     'YYYY-MM-DD HH24:MI:SS'),
    ROUND(
        EXTRACT(EPOCH FROM (
            TO_TIMESTAMP(end_time, 'YYYY-MM-DD HH24:MI:SS')
          - TO_TIMESTAMP(start_time,'YYYY-MM-DD HH24:MI:SS')
        )) / 60, 2
    ),
    distance_km::NUMERIC(6,2),
    fare_amount::NUMERIC(10,2)
FROM bronze.rides
WHERE ride_id IS NOT NULL;
