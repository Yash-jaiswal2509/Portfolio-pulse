from datetime import datetime, timedelta
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_KEY")
if not API_KEY:
    raise ValueError("No API key found. Please set ALPHAVANTAGE_KEY in .env")

TICKERS = ["AAPL", "MSFT", "GOOGL"]
START_DATE = (datetime.now() - timedelta(days=90)).strftime(
    "%Y-%m-%d"
)  # Get 3 months of data


data_dir = Path("data")
data_dir.mkdir(parents=True, exist_ok=True)

ts = TimeSeries(key=API_KEY, output_format="pandas")
all_data = []

for ticker in TICKERS:
    data, _ = ts.get_daily(symbol=ticker, outputsize="full")
    print(f"Fetched {data.shape[0]} rows for {ticker}")  # Debug print
    data.reset_index(inplace=True)
    data.rename(columns={"4. close": "close"}, inplace=True)
    data = data[["date", "close"]]
    data["ticker"] = ticker
    data = data[data["date"] >= pd.to_datetime(START_DATE)]
    print(
        f"After filtering from {START_DATE}, {data.shape[0]} rows remain for {ticker}"
    )  # Debug print
    all_data.append(data)


df_prices = pd.concat(all_data)
df_prices.sort_values(["date", "ticker"], inplace=True)
df_prices.to_csv(data_dir / "sample_prices.csv", index=False)
print(f"Saved {data_dir / 'sample_prices.csv'}")

# Simple weights - same allocation throughout the period
start_date = "2024-08-12"  # Match the start date of existing data
weights = pd.DataFrame(
    {
        "valid_from": [start_date, start_date, start_date],
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "weight": [0.4, 0.35, 0.25],
    }
)

weights.to_csv(data_dir / "portfolio_weights.csv", index=False)
print(f"Saved {data_dir / 'portfolio_weights.csv'}")
