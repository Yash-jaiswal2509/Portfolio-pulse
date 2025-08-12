CREATE TEMP VIEW v_returns AS
SELECT *
FROM (
        SELECT date,
            ticker,
(
                close / LAG(close) OVER (
                    PARTITION BY ticker
                    ORDER BY date
                ) -1.0
            ) AS ret
        FROM prices
    )
WHERE ret IS NOT NULL;