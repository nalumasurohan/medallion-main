DROP TABLE IF EXISTS silver.driver_shifts;
CREATE TABLE silver.driver_shifts (
    shift_id TEXT PRIMARY KEY,
    driver_id TEXT,
    shift_date DATE,
    hours_online NUMERIC(4,1),
    trips_completed INTEGER
);


INSERT INTO silver.driver_shifts
SELECT DISTINCT
    shift_id,
    driver_id,
    TO_DATE(shift_date, 'MM/DD/YYYY'),
    hours_online::NUMERIC(4,1),
    trips_completed::INT
FROM bronze.driver_shifts;

