import os
import argparse
import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger("ingest_b3")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def fetch_tickers(tickers, period="1mo", interval="1d"):
    """Download OHLCV data for the provided tickers using yfinance.

    Returns a tidy DataFrame with columns: ticker, date, open, high, low, close, volume
    """
    if not tickers:
        raise ValueError("Empty tickers list")

    tickers = [t.strip() for t in tickers if t and t.strip()]
    if not tickers:
        raise ValueError("Empty tickers list after cleaning")

    tickers_str = " ".join(tickers)
    logger.info("Downloading data for: %s", tickers_str)
    raw = yf.download(tickers=tickers_str, period=period, interval=interval, group_by="ticker", auto_adjust=False, progress=False)

    if raw is None or raw.empty:
        return pd.DataFrame()

    frames = []

    # When multiple tickers are requested yfinance returns a MultiIndex columns (ticker, field)
    if isinstance(raw.columns, pd.MultiIndex):
        for ticker in tickers:
            if ticker not in raw.columns.levels[0]:
                logger.warning("Ticker %s not present in downloaded data", ticker)
                continue
            sub = raw[ticker].copy()
            sub = sub.reset_index()
            sub["ticker"] = ticker
            frames.append(sub)
    else:
        # single ticker
        sub = raw.copy().reset_index()
        # try to infer ticker name from provided list
        sub["ticker"] = tickers[0]
        frames.append(sub)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Normalize column names
    df = df.rename(columns={
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })

    # Keep only required columns and ensure they exist
    required = ["ticker", "date", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]

    return df


def process_df(df, ingest_date=None):
    """Process dataframe and add data_ingestao column in YYYY-MM-DD format."""
    if ingest_date is None:
        ingest_date = datetime.now(timezone.utc).date().isoformat()
    else:
        # validate format YYYY-MM-DD
        try:
            datetime.strptime(ingest_date, "%Y-%m-%d")
        except Exception:
            raise ValueError("ingest_date must be in YYYY-MM-DD format")

    if df is None or df.empty:
        return df

    df = df.copy()
    # ensure date column is datetime64[ns] (not Python date objects) for parquet compatibility
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["data_ingestao"] = ingest_date

    # Enforce column types for Parquet consistency
    df["ticker"] = df["ticker"].astype(str)
    # numeric columns
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def write_parquet(df, out_dir=".", ingest_date=None):
    """Write dataframe to parquet under raw/data_ingestao=YYYY-MM-DD/dados.parquet"""
    if ingest_date is None:
        ingest_date = datetime.now(timezone.utc).date().isoformat()

    if df is None or df.empty:
        raise ValueError("No data to write")

    # reset index to avoid writing index-related metadata and ensure contiguous row groups
    df = df.reset_index(drop=True)

    # Coerce types for pyarrow: dates -> datetime64[ns], strings for ingest date
    if df["date"].dtype == object:
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["data_ingestao"] = df["data_ingestao"].astype(str)

    partition_dir = os.path.join(out_dir, "raw", f"data_ingestao={ingest_date}")
    os.makedirs(partition_dir, exist_ok=True)
    out_path = os.path.join(partition_dir, "dados.parquet")

    # debug info to help diagnose empty files
    logger.info("About to write Parquet: rows=%s, columns=%s", df.shape[0], list(df.columns))
    logger.debug("dtypes:\n%s", df.dtypes)
    logger.debug("sample:\n%s", df.head().to_string(index=False))

    # enforce dtypes for pyarrow and convert to pa.Table to avoid pandas->pyarrow ambiguities
    try:
        df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    except Exception:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").astype("datetime64[ns]")

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    df["ticker"] = df["ticker"].astype(str)
    df["data_ingestao"] = df["data_ingestao"].astype(str)

    # convert to pyarrow table and write explicitly
    table = pa.Table.from_pandas(df, preserve_index=False)
    logger.info("PyArrow table rows=%s, columns=%s", table.num_rows, table.num_columns)
    logger.info("Writing Parquet to %s", out_path)
    pq.write_table(table, out_path)

    return out_path


def main():
    parser = argparse.ArgumentParser(description="Ingest B3 tickers OHLCV and store as Parquet partitioned by ingest date")
    parser.add_argument("--tickers", required=True, help="Comma-separated list of tickers, e.g. PETR4.SA,VALE3.SA")
    parser.add_argument("--out-dir", default=".", help="Output base directory (default: current directory)")
    parser.add_argument("--date", default=None, help="Ingest date in YYYY-MM-DD (optional)")
    parser.add_argument("--period", default="1mo", help="yfinance period (default: 1mo)")
    parser.add_argument("--interval", default="1d", help="yfinance interval (default: 1d)")

    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    if not tickers:
        logger.error("No tickers provided")
        raise SystemExit(1)

    raw = fetch_tickers(tickers, period=args.period, interval=args.interval)
    if raw.empty:
        logger.error("No data downloaded for provided tickers")
        raise SystemExit(1)

    processed = process_df(raw, ingest_date=args.date)
    out_path = write_parquet(processed, out_dir=args.out_dir, ingest_date=(args.date or datetime.now(timezone.utc).date().isoformat()))

    logger.info("Ingest finished. Parquet file: %s", out_path)


if __name__ == "__main__":
    main()
