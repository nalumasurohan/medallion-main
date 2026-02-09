DROP TABLE IF EXISTS gold.dashboard_rides;

CREATE TABLE gold.dashboard_rides (
    ride_id TEXT PRIMARY KEY,

    -- Rider info
    rider_id TEXT,
    rider_name TEXT,
    rider_city TEXT,
    rider_rating NUMERIC(3,1),
    signup_date DATE,

    -- Driver info
    driver_id TEXT,
    driver_name TEXT,
    driver_city TEXT,
    driver_rating NUMERIC(3,1),

    -- Ride info
    ride_city TEXT,
    ride_status TEXT,
    request_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,

    waiting_time_min NUMERIC(6,2),
    ride_duration_min NUMERIC(6,2),

    distance_km NUMERIC(6,2),
    fare_amount NUMERIC(10,2),

    -- Payment info
    payment_method TEXT,
    payment_time TIMESTAMP,
    is_paid BOOLEAN,

    -- Derived metrics
    fare_per_km NUMERIC(10,2),
    ride_date DATE
);

INSERT INTO gold.dashboard_rides
SELECT
    r.ride_id,

    -- Rider
    ri.rider_id,
    ri.rider_name,
    ri.city AS rider_city,
    ri.rating AS rider_rating,
    ri.signup_date,

    -- Driver
    d.driver_id,
    d.driver_name,
    d.city AS driver_city,
    d.driver_rating,

    -- Ride
    r.city AS ride_city,
    r.ride_status,
    r.request_time,
    -- ✅ safe COALESCE for start_time
    COALESCE(r.start_time, r.request_time, TIMESTAMP '1970-01-01 00:00:00') AS start_time,
    -- ✅ safe COALESCE for end_time
    COALESCE(r.end_time, r.start_time, r.request_time, TIMESTAMP '1970-01-01 00:00:00') AS end_time,

    -- Waiting time (NEW)
    COALESCE(CASE
        WHEN r.start_time IS NOT NULL AND r.request_time IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (r.start_time - r.request_time)) / 60, 2)
        ELSE NULL
    END,0) AS waiting_time_min,

    -- Ride duration
    COALESCE(r.ride_duration_min,0),

    r.distance_km,
    r.fare_amount,

    -- Payment
    p.payment_method AS payment_method,
    COALESCE(p.payment_time, TIMESTAMP '1970-01-01 00:00:00') AS payment_time,
    CASE WHEN p.payment_id IS NULL THEN FALSE ELSE TRUE END AS is_paid,

    -- Derived metrics
    COALESCE(
        CASE
            WHEN r.distance_km > 0 THEN ROUND(r.fare_amount / r.distance_km, 2)
            ELSE NULL
        END, 0
    ) AS fare_per_km,

    DATE(r.request_time) AS ride_date

FROM silver.rides r
LEFT JOIN silver.riders ri ON r.rider_id = ri.rider_id
LEFT JOIN silver.drivers d ON r.driver_id = d.driver_id
LEFT JOIN silver.payments p ON r.ride_id = p.ride_id;
