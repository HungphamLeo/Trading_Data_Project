"""
Binance Klines Data Ingestion Script
Fetches hourly price data for cryptocurrencies from Binance API
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
from tenacity import retry, stop_after_attempt, wait_exponential

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent))

from shared.logger.python_logger import get_logger
from shared.utils.binance_setup import BinanceConfig

logger = get_logger('binance_ingestion')


class BinanceKlinesFetcher:
    """Fetch klines data from Binance API"""
    
    def __init__(self, config: BinanceConfig):
        self.config = config
        self.base_url = config.get_klines_url()
        self.session = requests.Session()
        self.output_dir = Path("/app/output/raw_rates")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def get_klines(
        self, 
        symbol: str, 
        interval: str, 
        start_ts_ms: int, 
        end_ts_ms: int
    ) -> List[List]:
        """
        Fetch klines data from Binance API
        
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            interval: Time interval (e.g., '1h')
            start_ts_ms: Start timestamp in milliseconds
            end_ts_ms: End timestamp in milliseconds
        
        Returns:
            List of kline data arrays
        """
        limit = self.config.max_klines_per_request
        all_klines = []
        current_start = start_ts_ms
        
        logger.info(f"Fetching {symbol} from {datetime.fromtimestamp(start_ts_ms/1000)} to {datetime.fromtimestamp(end_ts_ms/1000)}")
        
        while current_start < end_ts_ms:
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': current_start,
                'endTime': end_ts_ms,
                'limit': limit
            }
            
            try:
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    logger.warning(f"No data returned for {symbol} at {current_start}")
                    break
                
                all_klines.extend(data)
                
                # Update start time for next batch
                last_close_time = data[-1][6]  # closeTime
                current_start = last_close_time + 1
                
                logger.debug(f"Fetched {len(data)} klines for {symbol}, total: {len(all_klines)}")
                
                # Rate limiting
                time.sleep(0.2)
                
                # Break if we got less than limit (no more data)
                if len(data) < limit:
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching {symbol}: {e}")
                raise
        
        logger.info(f"Completed fetching {symbol}: {len(all_klines)} klines")
        return all_klines
    
    def klines_to_dataframe(self, symbol: str, klines: List[List]) -> pd.DataFrame:
        """
        Convert klines array to pandas DataFrame
        
        Args:
            symbol: Trading pair symbol
            klines: List of kline arrays from API
        
        Returns:
            DataFrame with structured kline data
        """
        if not klines:
            return pd.DataFrame()
        
        rows = []
        for k in klines:
            rows.append({
                'symbol': symbol,
                'open_time': int(k[0]),
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
                'close_time': int(k[6]),
                'quote_asset_volume': float(k[7]),
                'number_of_trades': int(k[8]),
                'taker_buy_base_asset_volume': float(k[9]),
                'taker_buy_quote_asset_volume': float(k[10]),
                'fetched_at': datetime.now(timezone.utc).isoformat()
            })
        
        df = pd.DataFrame(rows)
        
        # Add datetime columns for convenience
        df['open_time_dt'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
        df['close_time_dt'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)
        
        return df
    
    def save_to_parquet(self, df: pd.DataFrame, symbol: str, start_date: str, end_date: str):
        """Save DataFrame to parquet file"""
        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol}, skipping save")
            return
        
        filename = f"{symbol}__{start_date}__{end_date}.parquet"
        filepath = self.output_dir / filename
        
        df.to_parquet(filepath, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"Saved {len(df)} rows to {filepath}")
    
    def fetch_and_save(
        self, 
        symbol: str, 
        interval: str, 
        start_ts_ms: int, 
        end_ts_ms: int
    ) -> bool:
        """Fetch klines and save to parquet"""
        try:
            klines = self.get_klines(symbol, interval, start_ts_ms, end_ts_ms)
            
            if not klines:
                logger.warning(f"No klines data for {symbol}")
                return False
            
            df = self.klines_to_dataframe(symbol, klines)
            
            start_date = datetime.fromtimestamp(start_ts_ms/1000).strftime('%Y%m%d')
            end_date = datetime.fromtimestamp(end_ts_ms/1000).strftime('%Y%m%d')
            
            self.save_to_parquet(df, symbol, start_date, end_date)
            return True
            
        except Exception as e:
            logger.exception(f"Error processing {symbol}: {e}")
            return False


def extract_currencies_from_transactions(csv_path: str) -> Tuple[List[str], int, int]:
    """
    Extract unique currencies and time range from transactions CSV
    
    Args:
        csv_path: Path to transactions.csv
    
    Returns:
        Tuple of (currencies list, start_timestamp_ms, end_timestamp_ms)
    """
    logger.info(f"Reading transactions from {csv_path}")
    
    # Try reading Excel first, then CSV
    if csv_path.endswith('.xlsx'):
        df = pd.read_excel(csv_path)
    else:
        df = pd.read_csv(csv_path)
    
    logger.info(f"Loaded {len(df)} transactions")
    
    # Extract unique destination currencies
    currencies = df['destination_currency'].dropna().unique().tolist()
    currencies = sorted([c.upper() for c in currencies])
    
    logger.info(f"Found {len(currencies)} unique destination currencies: {currencies}")
    
    # Extract time range
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    start_ts = int(df['created_at'].min().timestamp() * 1000)
    end_ts = int(df['created_at'].max().timestamp() * 1000)
    
    logger.info(f"Time range: {datetime.fromtimestamp(start_ts/1000)} to {datetime.fromtimestamp(end_ts/1000)}")
    
    return currencies, start_ts, end_ts


def build_symbol_list(currencies: List[str]) -> List[str]:
    """
    Build list of Binance trading pairs
    
    Args:
        currencies: List of currency codes
    
    Returns:
        List of trading pair symbols (e.g., ['BTCUSDT', 'ETHUSDT'])
    """
    symbols = []
    
    for currency in currencies:
        currency = currency.upper()
        
        # Skip USDT itself (no need to fetch USDTUSDT)
        if currency == 'USDT':
            logger.info(f"Skipping {currency} (stablecoin, rate = 1.0)")
            continue
        
        # Skip fiat currencies that don't have USDT pairs on Binance
        if currency in ['VND', 'USD', 'EUR', 'GBP', 'JPY']:
            logger.warning(f"Skipping {currency} (fiat currency, not available on Binance)")
            continue
        
        # Build symbol
        symbol = f"{currency}USDT"
        symbols.append(symbol)
    
    logger.info(f"Built {len(symbols)} symbols to fetch: {symbols}")
    return symbols


def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("Starting Binance Klines Ingestion")
    logger.info("=" * 80)
    
    # Configuration
    config = BinanceConfig()
    fetcher = BinanceKlinesFetcher(config)
    
    # Paths
    data_dir = Path("/app/data")
    transactions_file = data_dir / "transactions.xlsx"
    
    # Check if file exists
    if not transactions_file.exists():
        transactions_file = data_dir / "transactions.csv"
        if not transactions_file.exists():
            logger.error(f"Transactions file not found at {data_dir}")
            sys.exit(1)
    
    # Extract currencies and time range
    currencies, start_ts_ms, end_ts_ms = extract_currencies_from_transactions(str(transactions_file))
    
    # Build symbol list
    symbols = build_symbol_list(currencies)
    
    if not symbols:
        logger.error("No valid symbols to fetch")
        sys.exit(1)
    
    # Fetch data for each symbol
    interval = '1h'
    success_count = 0
    failed_symbols = []
    
    for symbol in symbols:
        logger.info(f"Processing {symbol} ({symbols.index(symbol) + 1}/{len(symbols)})")
        
        success = fetcher.fetch_and_save(symbol, interval, start_ts_ms, end_ts_ms)
        
        if success:
            success_count += 1
        else:
            failed_symbols.append(symbol)
        
        # Small delay between symbols
        time.sleep(1)
    
    # Summary
    logger.info("=" * 80)
    logger.info("Ingestion Complete")
    logger.info(f"Successfully fetched: {success_count}/{len(symbols)} symbols")
    
    if failed_symbols:
        logger.warning(f"Failed symbols: {failed_symbols}")
    
    logger.info("=" * 80)

