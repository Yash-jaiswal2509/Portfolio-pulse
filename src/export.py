import argparse, sqlite3
from pathlib import Path
import pandas as pd
from jinja2 import Template
import numpy as np

HTML_TMPL = Template(
    "<!doctype html><html><body><h2>Summary</h2><ul>"
    "<li>Total Return: {{ total_return|round(4) }}</li>"
    "<li>Volatility (ann): {{ vol_ann|round(4) }}</li>"
    "<li>Sharpe (ann): {{ sharpe|round(3) }}</li>"
    "<li>Max Drawdown: {{ max_dd|round(4) }}</li>"
    "<li>30d Vol (ann): {{ vol_30d_ann|round(4) }}</li>"
    "<li>Anomaly Days (last 60): {{ anomalies_60 }}</li>"
    "</ul></body></html>"
)


def run_sql(conn, path: Path):
    conn.executescript(path.read_text())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--db",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "portfolio.sqlite"),
    )
    ap.add_argument(
        "--datadir", type=str, default=str(Path(__file__).resolve().parents[1] / "data")
    )
    ap.add_argument(
        "--outdir",
        type=str,
        default=str(Path(__file__).resolve().parents[1] / "reports"),
    )
    ap.add_argument("--use-sample", action="store_true")
    args = ap.parse_args()

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL;")

    run_sql(conn, Path(__file__).resolve().parents[1] / "sql/schema.sql")

    if args.use_sample:
        prices = pd.read_csv(Path(args.datadir) / "sample_prices.csv")
        weights = pd.read_csv(Path(args.datadir) / "portfolio_weights.csv")
    else:
        raise SystemExit("For demo, use --use-sample")

    prices.to_sql("prices", conn, if_exists="replace", index=False)
    weights.to_sql("weights", conn, if_exists="replace", index=False)

    run_sql(conn, Path(__file__).resolve().parents[1] / "sql/views.sql")
    run_sql(conn, Path(__file__).resolve().parents[1] / "sql/weights_resolved.sql")
    run_sql(conn, Path(__file__).resolve().parents[1] / "sql/portfolio_views.sql")

    df_port = pd.read_sql_query("SELECT * FROM v_portfolio_returns", conn)
    df_path = pd.read_sql_query("SELECT * FROM v_portfolio_path", conn)
    df_dd = pd.read_sql_query("SELECT * FROM v_drawdown", conn)
    df_risk = pd.read_sql_query("SELECT * FROM v_risk_30d", conn)
    df_anom = pd.read_sql_query("SELECT * FROM v_anomalies", conn)

    port_ret = df_port["port_ret"].to_numpy()
    vol_ann = float(np.std(port_ret, ddof=0) * np.sqrt(252))
    mean_d = float(port_ret.mean())
    sharpe = float(0 if vol_ann == 0 else (mean_d * np.sqrt(252)) / vol_ann)
    max_dd = float(df_dd["drawdown"].min())
    total_return = float(df_path["port_cum"].iloc[-1] - 1.0)
    last_var = (
        float(df_risk["var_30d"].dropna().iloc[-1])
        if not df_risk["var_30d"].dropna().empty
        else 0.0
    )
    vol_30d_ann = float(np.sqrt(max(0.0, last_var)) * np.sqrt(252))
    anomalies_60 = int(df_anom.tail(60)["is_anomaly"].sum())

    with pd.ExcelWriter(out / "sql_first_report.xlsx") as wr:
        pd.read_sql_query("SELECT * FROM prices", conn).to_excel(
            wr, index=False, sheet_name="Prices"
        )
        pd.read_sql_query("SELECT * FROM weights", conn).to_excel(
            wr, index=False, sheet_name="Weights"
        )
        pd.read_sql_query("SELECT * FROM v_returns", conn).to_excel(
            wr, index=False, sheet_name="Returns"
        )
        pd.read_sql_query("SELECT * FROM v_weights_by_date_resolved", conn).to_excel(
            wr, index=False, sheet_name="WeightsByDate"
        )
        df_port.to_excel(wr, index=False, sheet_name="PortfolioRet")
        df_path.to_excel(wr, index=False, sheet_name="PortfolioPath")
        df_dd.to_excel(wr, index=False, sheet_name="Drawdown")
        df_risk.to_excel(wr, index=False, sheet_name="Risk30d")
        df_anom.to_excel(wr, index=False, sheet_name="Anomalies")
        pd.DataFrame(
            [
                {
                    "total_return": total_return,
                    "vol_ann": vol_ann,
                    "sharpe": sharpe,
                    "max_drawdown": max_dd,
                    "vol_30d_ann": vol_30d_ann,
                    "anomalies_60": anomalies_60,
                }
            ]
        ).to_excel(wr, index=False, sheet_name="Summary")

    (out / "sql_first_summary.html").write_text(
        HTML_TMPL.render(
            total_return=total_return,
            vol_ann=vol_ann,
            sharpe=sharpe,
            max_dd=max_dd,
            vol_30d_ann=vol_30d_ann,
            anomalies_60=anomalies_60,
        )
    )


if __name__ == "__main__":
    main()
