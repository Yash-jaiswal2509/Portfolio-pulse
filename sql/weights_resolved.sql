DROP VIEW IF EXISTS v_weights_by_date_resolved;
CREATE TEMP VIEW v_weights_by_date_resolved AS WITH d AS (
    SELECT DISTINCT date
    FROM prices
),
x AS(
    SELECT d.date,
        w.ticker,
        w.weight,
        ROW_NUMBER() OVER (
            PARTITION BY d.date,
            w.ticker
            ORDER BY w.valid_from DESC
        ) rn
    FROM d
        LEFT JOIN weights w ON w.valid_from <= d.date
)
SELECT date,
    ticker,
    weight
FROM x
WHERE rn = 1;