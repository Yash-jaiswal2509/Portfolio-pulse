DROP VIEW IF EXISTS v_portfolio_returns;
CREATE TEMP VIEW v_portfolio_returns AS
SELECT r.date,
    SUM(COALESCE(w.weight, 0) * r.ret) AS port_ret
FROM v_returns r
    LEFT JOIN v_weights_by_date_resolved w ON w.date = r.date
    AND w.ticker = r.ticker
GROUP BY r.date
ORDER BY r.date;
DROP VIEW IF EXISTS v_portfolio_path;
CREATE TEMP VIEW v_portfolio_path AS WITH RECURSIVE path(date, port_cum) AS (
    SELECT date,
        1.0
    FROM (
            SELECT date
            FROM v_portfolio_returns
            ORDER BY date
            LIMIT 1
        )
    UNION ALL
    SELECT v.date,
        path.port_cum *(1.0 + v.port_ret)
    FROM v_portfolio_returns v
        JOIN path ON v.date =(
            SELECT date
            FROM v_portfolio_returns
            WHERE date > path.date
            ORDER BY date
            LIMIT 1
        )
)
SELECT *
FROM path;
DROP VIEW IF EXISTS v_drawdown;
CREATE TEMP VIEW v_drawdown AS WITH RECURSIVE dd(date, port_cum, peak, drawdown) AS (
    SELECT date,
        port_cum,
        port_cum,
        0.0
    FROM (
            SELECT *
            FROM v_portfolio_path
            ORDER BY date
            LIMIT 1
        )
    UNION ALL
    SELECT p.date,
        p.port_cum,
        CASE
            WHEN p.port_cum > dd.peak THEN p.port_cum
            ELSE dd.peak
        END,
        (
            p.port_cum /(
                CASE
                    WHEN p.port_cum > dd.peak THEN p.port_cum
                    ELSE dd.peak
                END
            )
        ) -1.0
    FROM v_portfolio_path p
        JOIN dd ON p.date =(
            SELECT date
            FROM v_portfolio_path
            WHERE date > dd.date
            ORDER BY date
            LIMIT 1
        )
)
SELECT *
FROM dd;
DROP VIEW IF EXISTS v_risk_30d;
CREATE TEMP VIEW v_risk_30d AS
SELECT r.date,
    AVG(r.port_ret) OVER (
        ORDER BY r.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS mean_30d,
    AVG(r.port_ret * r.port_ret) OVER (
        ORDER BY r.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS mean2_30d,
    (
        AVG(r.port_ret * r.port_ret) OVER (
            ORDER BY r.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) -(
            AVG(r.port_ret) OVER (
                ORDER BY r.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
        ) *(
            AVG(r.port_ret) OVER (
                ORDER BY r.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            )
        )
    ) AS var_30d
FROM v_portfolio_returns r;
DROP VIEW IF EXISTS v_anomalies;
CREATE TEMP VIEW v_anomalies AS
SELECT r.date,
    r.port_ret,
    v.var_30d,
    CASE
        WHEN v.var_30d IS NOT NULL
        AND (r.port_ret * r.port_ret) >(4.0 * v.var_30d) THEN 1
        ELSE 0
    END AS is_anomaly
FROM v_portfolio_returns r
    LEFT JOIN v_risk_30d v ON v.date = r.date;