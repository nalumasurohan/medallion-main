CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.dq_issues (
    run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name TEXT,
    issue_type TEXT,
    issue_count INT,
    sample_ids TEXT
);
INSERT INTO audit.dq_issues (
    table_name,
    issue_type,
    issue_count,
    sample_ids
)
SELECT
    'riders',
    'DUPLICATE_RIDER_ID',
    COUNT(*) - COUNT(DISTINCT rider_id),
    STRING_AGG(rider_id::TEXT, ',')
FROM bronze.riders;

INSERT INTO audit.dq_issues (
    table_name, issue_type, issue_count, sample_ids
)
SELECT
    'drivers',
    'MISSING_CITY',
    COUNT(*),
    STRING_AGG(driver_id::TEXT, ',')
FROM bronze.drivers
WHERE city IS NULL;

INSERT INTO audit.dq_issues (
    table_name, issue_type, issue_count, sample_ids
)
SELECT
    'rides',
    'INVALID_DRIVER_ID',
    COUNT(*),
    STRING_AGG(ride_id::TEXT, ',')
FROM silver.rides r
LEFT JOIN silver.drivers d ON r.driver_id = d.driver_id
WHERE d.driver_id IS NULL;

INSERT INTO audit.dq_issues (
    table_name, issue_type, issue_count, sample_ids
)
SELECT
    'payments',
    'NEGATIVE_AMOUNT',
    COUNT(*),
    STRING_AGG(payment_id::TEXT, ',')
FROM silver.payments;

INSERT INTO audit.dq_issues (
    table_name, issue_type, issue_count, sample_ids
)
SELECT
    'driver_shifts',
    'NEGATIVE_HOURS',
    COUNT(*),
    STRING_AGG(shift_id::TEXT, ',')
FROM silver.driver_shifts
WHERE hours_online < 0;