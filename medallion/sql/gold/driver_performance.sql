DROP TABLE IF EXISTS gold.driver_performance;

CREATE TABLE gold.driver_performance AS
SELECT
    d.driver_id,
    d.driver_name,
    d.city,

    COUNT(r.ride_id) AS total_rides,

    ROUND(SUM(r.fare_amount)::NUMERIC, 2) AS total_earnings,
    ROUND(AVG(r.fare_amount)::NUMERIC, 2) AS avg_fare_per_ride,

    ROUND(SUM(s.hours_online)::NUMERIC, 2) AS total_hours_online,

    ROUND(
        (
            COUNT(r.ride_id)::NUMERIC
            / NULLIF(SUM(s.hours_online), 0)::NUMERIC
        ),
        2
    ) AS rides_per_hour

FROM silver.drivers d
LEFT JOIN silver.rides r
    ON d.driver_id = r.driver_id
LEFT JOIN silver.driver_shifts s
    ON d.driver_id = s.driver_id
GROUP BY d.driver_id, d.driver_name, d.city;