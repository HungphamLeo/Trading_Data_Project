"""
Binance Klines Data Ingestion with Prefect
Prefect replaces Tenacity for retry logic.
"""

import os
import sys
import time
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import requests
import pandas as pd

from prefect import task, flow, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))

from shared.logger.python_logger import get_logger
from shared.utils.ingestion_utils.binance_setup import BinanceConfig

logger = get_logger("binance_ingestion")


# -----------------------------------------------------
#   Core Fetcher (NO RETRY HERE) — pure functionality
# -----------------------------------------------------
class BinanceKlinesFetcher:
    """Fetch klines data from Binance API"""

    def __init__(self, config: BinanceConfig):
        self.config = config
        self.base_url = config.get_klines_url()
        self.session = requests.Session()
        self.output_dir = Path("/app/output/raw_rates")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_klines_once(
        self,
        symbol: str,
        interval: str,
        start_ts_ms: int,
        end_ts_ms: int
    ) -> List[List]:
        """Single-attempt fetch: retry handled by Prefect"""
        limit = self.config.max_klines_per_request
        all_klines = []
        current_start = start_ts_ms

        logger.info(
            f"Fetching {symbol} from {datetime.fromtimestamp(start_ts_ms/1000)} "
            f"to {datetime.fromtimestamp(end_ts_ms/1000)}"
        )

        while current_start < end_ts_ms:
            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start,
                "endTime": end_ts_ms,
                "limit": limit,
            }

            try:
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if not data:
                    logger.warning(f"No data returned for {symbol} at {current_start}")
                    break

                all_klines.extend(data)

                last_close_time = data[-1][6]
                current_start = last_close_time + 1

                logger.debug(
                    f"Fetched {len(data)} klines for {symbol}, total: {len(all_klines)}"
                )

                time.sleep(0.2)

                if len(data) < limit:
                    break

            except Exception as e:
                logger.error(f"Error fetching {symbol}: {e}")
                raise

        logger.info(f"Completed fetching {symbol}: {len(all_klines)} klines")
        return all_klines

    def klines_to_dataframe(self, symbol: str, klines: List[List]) -> pd.DataFrame:
        if not klines:
            return pd.DataFrame()

        rows = []
        for k in klines:
            rows.append({
                "symbol": symbol,
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
                "quote_asset_volume": float(k[7]),
                "number_of_trades": int(k[8]),
                "taker_buy_base_asset_volume": float(k[9]),
                "taker_buy_quote_asset_volume": float(k[10]),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })

        df = pd.DataFrame(rows)
        df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time_dt"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        return df

    def save_to_parquet(self, df: pd.DataFrame, symbol: str, start_date: str, end_date: str):
        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol}, skipping save")
            return

        filename = f"{symbol}__{start_date}__{end_date}.parquet"
        filepath = self.output_dir / filename

        df.to_parquet(filepath, index=False, engine="pyarrow", compression="snappy")
        logger.info(f"Saved {len(df)} rows to {filepath}")


# -----------------------------------------------------
# Prefect Tasks (retry logic HERE)
# -----------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=5,
    name="fetch_binance_klines",
)
def task_fetch_klines(fetcher: BinanceKlinesFetcher, symbol: str, interval: str, start_ms: int, end_ms: int):
    return fetcher.get_klines_once(symbol, interval, start_ms, end_ms)


@task(name="convert_to_df")
def task_to_dataframe(fetcher: BinanceKlinesFetcher, symbol: str, klines: List[List]):
    return fetcher.klines_to_dataframe(symbol, klines)


@task(name="save_parquet")
def task_save_parquet(fetcher: BinanceKlinesFetcher, df: pd.DataFrame, symbol: str, start_ms: int, end_ms: int):
    start_date = datetime.fromtimestamp(start_ms / 1000).strftime("%Y%m%d")
    end_date = datetime.fromtimestamp(end_ms / 1000).strftime("%Y%m%d")
    fetcher.save_to_parquet(df, symbol, start_date, end_date)


# -----------------------------------------------------
#   Utility functions
# -----------------------------------------------------

def extract_currencies_from_transactions(csv_path: str):
    logger.info(f"Reading transactions from {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} transactions")

    currencies = sorted(df["destination_currency"].dropna().str.upper().unique().tolist())

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    start_ts = int(df["created_at"].min().timestamp() * 1000)
    end_ts = int(df["created_at"].max().timestamp() * 1000)

    return currencies, start_ts, end_ts


def build_symbol_list(currencies: List[str]) -> List[str]:
    symbols = []
    for c in currencies:
        if c == "USDT":
            continue
        if c in ["VND", "USD", "EUR", "GBP", "JPY"]:
            continue
        symbols.append(f"{c}USDT")
    return symbols


# -----------------------------------------------------
# Prefect Flow — orchestrating entire ingestion
# -----------------------------------------------------

@flow(name="binance-klines-ingestion-flow")
def binance_ingestion_flow():
    log = get_run_logger()
    log.info("Starting Binance Ingestion (Prefect)")

    config = BinanceConfig()
    fetcher = BinanceKlinesFetcher(config)

    csv_path = "/app/data/transactions.csv"
    currencies, start_ms, end_ms = extract_currencies_from_transactions(csv_path)
    symbols = build_symbol_list(currencies)

    interval = "1h"

    for symbol in symbols:
        log.info(f"Processing symbol: {symbol}")

        klines = task_fetch_klines(fetcher, symbol, interval, start_ms, end_ms)
        df = task_to_dataframe(fetcher, symbol, klines)
        task_save_parquet(fetcher, df, symbol, start_ms, end_ms)

        time.sleep(1)

    log.info("Ingestion done!")


if __name__ == "__main__":
    binance_ingestion_flow()
