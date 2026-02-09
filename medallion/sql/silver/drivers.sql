DROP TABLE IF EXISTS silver.drivers;

CREATE TABLE silver.drivers (
    driver_id TEXT PRIMARY KEY,
    driver_name TEXT,
    city TEXT,
    join_date DATE,
    driver_rating NUMERIC(3,1)
);
INSERT INTO silver.drivers
SELECT DISTINCT
    driver_id,
    INITCAP(TRIM(driver_name)),
    INITCAP(TRIM(city)),
    TO_DATE(join_date, 'MM/DD/YYYY'),
    driver_rating::NUMERIC(3,1)
FROM bronze.drivers
WHERE driver_id IS NOT NULL;
