DROP TABLE IF EXISTS silver.payments;
CREATE TABLE silver.payments (
    payment_id TEXT PRIMARY KEY,
    ride_id TEXT,
    payment_method TEXT,
    payment_time TIMESTAMP
);


INSERT INTO silver.payments
SELECT DISTINCT
    payment_id,
    ride_id,
    UPPER(payment_method),
    TO_TIMESTAMP(payment_time, 'YYYY-MM-DD HH24:MI:SS')
FROM bronze.payments;


