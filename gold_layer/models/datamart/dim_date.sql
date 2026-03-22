-- Dimension: date (generated, bukan dari source)
-- Range: 2024-01-01 sampai 2026-12-31
WITH date_spine AS (
    SELECT UNNEST(generate_series(DATE '2024-01-01', DATE '2026-12-31', INTERVAL 1 DAY))::DATE AS full_date
)

SELECT
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    MONTH(full_date) AS month,
    DAY(full_date) AS day,
    DAYOFWEEK(full_date) AS day_of_week,
    strftime(full_date, '%B') AS month_name,
    strftime(full_date, '%A') AS day_name
FROM date_spine
