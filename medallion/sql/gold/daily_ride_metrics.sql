DROP TABLE IF EXISTS gold.daily_ride_metrics;

CREATE TABLE gold.daily_ride_metrics AS
SELECT
    DATE(request_time) AS ride_date,
    city,
    COUNT(*) AS total_rides,
    COUNT(*) FILTER (WHERE ride_status = 'COMPLETED') AS completed_rides,
    COUNT(*) FILTER (WHERE ride_status = 'CANCELLED') AS cancelled_rides,
    ROUND(SUM(fare_amount), 2) AS total_revenue,
    ROUND(AVG(distance_km), 2) AS avg_distance_km,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM silver.rides
GROUP BY DATE(request_time), city;