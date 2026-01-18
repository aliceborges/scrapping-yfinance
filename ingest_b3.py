import os
import argparse
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.exceptions import ClientError

load_dotenv()

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

def df_to_parquet_bytes(df, ingest_date=None):
    """Convert processed DataFrame to parquet bytes (in-memory)."""
    if ingest_date is None:
        ingest_date = datetime.now(timezone.utc).date().isoformat()

    if df is None or df.empty:
        raise ValueError("No data to convert to parquet")

    # reset index and coerce types as before
    df = df.reset_index(drop=True).copy()

    try:
        df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    except Exception:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").astype("datetime64[ns]")

    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

    df["ticker"] = df["ticker"].astype(str)
    df["data_ingestao"] = df["data_ingestao"].astype(str)

    table = pa.Table.from_pandas(df, preserve_index=False)

    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    buf = sink.getvalue().to_pybytes()

    logger.info("Converted DataFrame to in-memory Parquet bytes: rows=%s, columns=%s", table.num_rows, table.num_columns)
    return buf


def upload_parquet_bytes_to_s3(parquet_bytes, bucket_name, ingest_date, region_name=None):
    """Upload parquet bytes directly to S3 under raw/data_ingestao=YYYY-MM-DD/file.parquet"""
    session_kwargs = {}
    if region_name:
        session_kwargs['region_name'] = region_name
    session = boto3.Session(**session_kwargs) if session_kwargs else boto3.Session()

    creds = session.get_credentials()
    if not creds:
        logger.error("No AWS credentials found in environment or default chain. Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set or an IAM role is available.")
        raise ValueError("AWS credentials not found")

    frozen = creds.get_frozen_credentials()
    access_key = getattr(frozen, 'access_key', None)
    secret_key = getattr(frozen, 'secret_key', None)

    if not access_key or not secret_key:
        logger.error("AWS credentials appear incomplete (missing access key or secret). Check environment or AWS config.")
        raise ValueError("AWS credentials incomplete")

    masked = f"{access_key[:4]}****{access_key[-4:]}" if len(access_key) > 8 else "****"
    logger.info("Using AWS access key: %s (masked)", masked)

    s3 = session.client('s3')

    # verify bucket exists when possible; if HeadBucket is forbidden, log and continue to attempt upload
    try:
        bucket_check = ensure_bucket_exists(s3, bucket_name)
    except ValueError as e:
        logger.exception("Bucket check failed: %s", e)
        raise

    s3_key = f"raw/data_ingestao={ingest_date}/file.parquet"

    logger.info("Uploading parquet bytes to s3://%s/%s (bucket_check=%s) - size=%s bytes", bucket_name, s3_key, bucket_check, len(parquet_bytes))

    # run a small put/delete test before attempting the full file upload to provide clearer errors
    try:
        test_s3_put_permission(s3, bucket_name)
    except Exception:
        logger.error("Write permission test failed; aborting upload to avoid overwriting or partial uploads.")
        raise

    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_bytes)
    except ClientError as e:
        err = e.response.get('Error', {})
        logger.exception("Failed to upload bytes to S3: %s - %s", err.get('Code'), err.get('Message'))
        raise

    return f"s3://{bucket_name}/{s3_key}"


def ensure_bucket_exists(s3_client, bucket_name):
    """Return True if bucket exists and is accessible.

    If HeadBucket returns 403 (forbidden/access denied) we return False so caller
    can decide to attempt the upload (some principals can PutObject but not HeadBucket).
    For other errors we raise a ValueError with details.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        msg = e.response.get('Error', {}).get('Message')
        if str(code) in ("403", "Forbidden", "AccessDenied"):
            logger.warning("HeadBucket returned %s for bucket '%s' (may be cross-account or access denied). Will attempt upload and rely on PutObject permissions.", code, bucket_name)
            return False
        raise ValueError(f"S3 bucket '{bucket_name}' is not accessible: {code} - {msg}")


def test_s3_put_permission(s3_client, bucket_name):
    """Try to write and delete a tiny object to verify PutObject/DeleteObject permissions.

    Returns True if succeeded, otherwise raises the caught exception for clearer reporting.
    """
    test_key = "raw/_permission_test.txt"
    try:
        s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=b"ok")
        s3_client.delete_object(Bucket=bucket_name, Key=test_key)
        logger.info("S3 put/delete test succeeded for bucket %s", bucket_name)
        return True
    except ClientError as e:
        err = e.response.get('Error', {})
        code = err.get('Code')
        msg = err.get('Message')
        logger.error("S3 put/delete test failed: %s - %s", code, msg)
        raise


def main():
    parser = argparse.ArgumentParser(description="Ingest B3 tickers OHLCV and store as Parquet partitioned by ingest date")
    parser.add_argument("--tickers", required=True, help="Comma-separated list of tickers, e.g. PETR4.SA,VALE3.SA")
    parser.add_argument("--out-dir", default=".", help="Output base directory (default: current directory)")
    parser.add_argument("--date", default=None, help="Ingest date in YYYY-MM-DD (optional)")
    parser.add_argument("--period", default="1mo", help="yfinance period (default: 1mo)")
    parser.add_argument("--interval", default="1d", help="yfinance interval (default: 1d)")
    parser.add_argument("--s3-bucket", default=None, help="S3 bucket name to upload the parquet file (optional; will use S3_BUCKET_NAME env if not provided)")
    parser.add_argument("--aws-region", default=None, help="AWS region name (optional)")

    args = parser.parse_args()

    result = run_ingest(
        tickers=args.tickers,
        out_dir=args.out_dir,
        ingest_date=args.date,
        period=args.period,
        interval=args.interval,
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region,
    )

    if isinstance(result, dict) and result.get("statusCode", 200) >= 400:
        raise SystemExit(1)


def run_ingest(tickers, out_dir=".", ingest_date=None, period="1mo", interval="1d", s3_bucket=None, aws_region=None):
    """Reusable ingestion function.

    Parameters accepted as either a comma-separated string or a list of tickers.
    Returns a dict with statusCode and body for easier use from Lambda.
    """
    try:
        # Normalize tickers param
        if isinstance(tickers, str):
            tickers_list = [t.strip() for t in tickers.split(",") if t.strip()]
        elif isinstance(tickers, (list, tuple)):
            tickers_list = [str(t).strip() for t in tickers if t and str(t).strip()]
        else:
            tickers_list = []

        if not tickers_list:
            msg = "No tickers provided"
            logger.error(msg)
            return {"statusCode": 400, "body": msg}

        raw = fetch_tickers(tickers_list, period=period, interval=interval)
        if raw.empty:
            msg = "No data downloaded for provided tickers"
            logger.error(msg)
            return {"statusCode": 204, "body": msg}

        processed = process_df(raw, ingest_date=ingest_date)

        ingest_date_val = ingest_date or datetime.now(timezone.utc).date().isoformat()
        parquet_bytes = df_to_parquet_bytes(processed, ingest_date=ingest_date_val)

        bucket = s3_bucket or os.getenv("S3_BUCKET_NAME")
        aws_region = aws_region or os.getenv("AWS_REGION")

        if bucket:
            try:
                s3_uri = upload_parquet_bytes_to_s3(parquet_bytes, bucket, ingest_date_val, region_name=aws_region)
                logger.info("Upload successful: %s", s3_uri)
                return {"statusCode": 200, "body": {"message": "Upload successful", "s3_uri": s3_uri}}
            except Exception as e:
                logger.exception("S3 upload failed")
                return {"statusCode": 500, "body": str(e)}
        else:
            logger.info("No S3 bucket configured (no s3_bucket arg and no S3_BUCKET_NAME env). Skipping upload and not creating local file.")
            return {"statusCode": 200, "body": "Ingest finished (no upload)"}

    except Exception as exc:
        logger.exception("Unexpected error in run_ingest")
        return {"statusCode": 500, "body": str(exc)}


def lambda_handler(event, context):
    """AWS Lambda handler.

    Expected event keys (all optional but at least `tickers` is recommended):
      - tickers: comma-separated string or list of ticker symbols
      - date: YYYY-MM-DD
      - period: yfinance period (e.g. '1mo')
      - interval: yfinance interval (e.g. '1d')
      - s3_bucket: bucket name (overrides S3_BUCKET_NAME env var)
      - aws_region: AWS region

    Returns a dict compatible with Lambda proxy integration.
    """
    logger.info("Lambda invoked with event: %s", event)

    # map event to params
    tickers = event.get("tickers") or event.get("ticker")
    ingest_date = event.get("date") or event.get("ingest_date")
    period = event.get("period")
    interval = event.get("interval")
    s3_bucket = event.get("s3_bucket") or event.get("bucket")
    aws_region = event.get("aws_region") or event.get("region")

    result = run_ingest(
        tickers=tickers,
        out_dir=event.get("out_dir", "."),
        ingest_date=ingest_date,
        period=period or "1mo",
        interval=interval or "1d",
        s3_bucket=s3_bucket,
        aws_region=aws_region,
    )

    # Ensure Lambda-friendly return structure
    status = result.get("statusCode", 200) if isinstance(result, dict) else 200
    body = result.get("body", "") if isinstance(result, dict) else result

    # If body is a dict, keep it as-is; otherwise stringify
    if not isinstance(body, (dict, list)):
        body = {"message": body}

    return {"statusCode": status, "body": body}


if __name__ == "__main__":
    main()
