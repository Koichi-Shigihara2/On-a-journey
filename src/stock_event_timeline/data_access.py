import sqlite3
from typing import Optional
from datetime import datetime
import pandas as pd
import yfinance as yf

from .config import DB_PATH


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS price_history (
          ticker TEXT,
          date TEXT,
          open REAL,
          high REAL,
          low REAL,
          close REAL,
          adj_close REAL,
          volume INTEGER,
          PRIMARY KEY (ticker, date)
        )
        """
    )
    conn.commit()
    conn.close()


def fetch_and_store_price_history(ticker: str, period_years: int = 5) -> pd.DataFrame:
    init_db()
    end = datetime.utcnow()
    start = datetime(end.year - period_years, end.month, end.day)
    df = yf.download(ticker, start=start, end=end, auto_adjust=False)
    if df.empty:
        return df

    df = df.reset_index()
    df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )
    df["ticker"] = ticker.upper()
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    conn = get_connection()
    df[["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]].to_sql(
        "price_history", conn, if_exists="append", index=False
    )
    conn.close()
    return df


def load_price_history(ticker: str, period_years: int = 5) -> pd.DataFrame:
    init_db()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT date, open, high, low, close, adj_close, volume "
        "FROM price_history WHERE ticker = ? ORDER BY date",
        (ticker.upper(),),
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return fetch_and_store_price_history(ticker, period_years)

    df = pd.DataFrame(
        rows,
        columns=["date", "open", "high", "low", "close", "adj_close", "volume"],
    )
    df["date"] = pd.to_datetime(df["date"])
    return df
