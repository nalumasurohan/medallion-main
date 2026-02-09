CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.riders;

CREATE TABLE silver.riders (
    rider_id TEXT PRIMARY KEY,
    rider_name TEXT,
    city TEXT,
    signup_date DATE,
    rating NUMERIC(3,1)
);

INSERT INTO silver.riders
SELECT DISTINCT
    rider_id,
    INITCAP(TRIM(rider_name)),
    INITCAP(TRIM(city)),
    TO_DATE(signup_date, 'MM/DD/YYYY'),
    rating::NUMERIC(3,1)
FROM bronze.riders
WHERE rider_id IS NOT NULL;
