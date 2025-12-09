import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
import requests
import pandas as pd

from shared.logger.python_logger import get_logger
from shared.utils.files.binance_setup import BinanceConfig
from platforms.ingestion.crypto.binance.extract.binance_dto import (
    BinanceGetKlinesParams,
    KlinesResponse,
    IngestionConfig,
    TransactionData,
    SymbolProcessingResult,
    IngestionResult,
    DataFrameOutput,
)


class BinanceKlinesFetcher:
    def __init__(self, config: BinanceConfig, logger_name: str = "binance_ingestion"):
        self.config = config
        self.logger = get_logger(logger_name)
        self.base_url = config.get_klines_url()
        self.session = requests.Session()
        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_klines_once(self, params: BinanceGetKlinesParams) -> KlinesResponse:
        params.validate()
        
        limit = self.config.max_klines_per_request
        all_klines = []
        current_start = params.start_ts_ms
        start_dt = datetime.fromtimestamp(params.start_ts_ms / 1000)
        end_dt = datetime.fromtimestamp(params.end_ts_ms / 1000)
        
        self.logger.info(f"Fetching {params.symbol} ({params.interval}) {start_dt} -> {end_dt}")

        while current_start < params.end_ts_ms:
            api_params = {
                "symbol": params.symbol,
                "interval": params.interval,
                "startTime": current_start,
                "endTime": params.end_ts_ms,
                "limit": limit,
            }

            try:
                response = self.session.get(
                    self.base_url, 
                    params=api_params, 
                    timeout=(self.config.connect_timeout, self.config.read_timeout)
                )
                response.raise_for_status()
                data = response.json()

                if not data:
                    break

                all_klines.extend(data)
                last_close_time = data[-1][6]
                current_start = last_close_time + 1
                time.sleep(0.2)

                if len(data) < limit:
                    break

            except requests.exceptions.RequestException as e:
                self.logger.error(f"API error {params.symbol}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error {params.symbol}: {e}")
                raise

        self.logger.info(f"Fetched {params.symbol}: {len(all_klines)} klines")
        
        return KlinesResponse(
            symbol=params.symbol,
            interval=params.interval,
            klines=all_klines,
            total_count=len(all_klines),
            start_time=start_dt,
            end_time=end_dt
        )

    def klines_to_dataframe(self, response: KlinesResponse) -> pd.DataFrame:
        if response.is_empty():
            return pd.DataFrame()

        rows = []
        for k in response.klines:
            rows.append({
                "symbol": response.symbol,
                "interval": response.interval,
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
                "ignore": int(k[11]),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })

        df = pd.DataFrame(rows)
        df["open_time_dt"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        df["close_time_dt"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
        return df

    def save_to_parquet(self, df: pd.DataFrame, output: DataFrameOutput) -> DataFrameOutput:
        if df.empty:
            return output

        filename = f"{output.symbol}_{output.interval}_{output.file_path}.parquet"
        filepath = self.output_dir / filename

        df.to_parquet(filepath, index=False, engine="pyarrow", compression=output.compression)
        
        output.file_path = str(filepath)
        output.row_count = len(df)
        output.size_bytes = filepath.stat().st_size
        
        self.logger.info(f"Saved {output.symbol}: {output.row_count} rows ({output.size_bytes} bytes)")
        return output


def extract_currencies_from_transactions(csv_path: str, logger) -> TransactionData:
    logger.info(f"Reading {csv_path}")

    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} transactions")

    required_columns = ["destination_currency", "created_at"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    currencies = sorted(df["destination_currency"].dropna().str.upper().unique().tolist())
    logger.info(f"Found {len(currencies)} currencies")

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    start_ts = int(df["created_at"].min().timestamp() * 1000)
    end_ts = int(df["created_at"].max().timestamp() * 1000)

    return TransactionData(
        currencies=currencies,
        start_timestamp_ms=start_ts,
        end_timestamp_ms=end_ts,
        total_records=len(df)
    )


def build_symbol_list(tx_data: TransactionData, config: BinanceConfig, logger) -> List[str]:
    symbols = []
    for c in tx_data.currencies:
        if config.is_stablecoin(c) or config.should_exclude_currency(c):
            continue
        symbols.append(f"{c}USDT")
    
    logger.info(f"Built symbol list: {len(symbols)} symbols")
    return symbols


def run_ingestion(
    csv_path: str,
    interval: str = "1h",
    output_dir: Optional[str] = None
) -> IngestionResult:
    start_time = datetime.now()
    result = IngestionResult(
        total_symbols=0,
        successful_count=0,
        failed_count=0,
        start_time=start_time
    )
    
    try:
        config = BinanceConfig()
        if output_dir:
            config.output_dir = output_dir
        
        ing_config = IngestionConfig(
            csv_path=csv_path,
            interval=interval,
            output_dir=output_dir
        )
        ing_config.validate()
        
        logger = get_logger("binance_ingestion")
        fetcher = BinanceKlinesFetcher(config)
        
        logger.info("Starting ingestion")
        
        tx_data = extract_currencies_from_transactions(csv_path, logger)
        
        if not tx_data.currencies:
            logger.warning("No currencies found")
            result.end_time = datetime.now()
            return result
        
        symbols = build_symbol_list(tx_data, config, logger)
        
        if not symbols:
            logger.warning("No symbols generated")
            result.end_time = datetime.now()
            return result
        
        result.total_symbols = len(symbols)
        logger.info(f"Processing {len(symbols)} symbols")
        
        for idx, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"[{idx}/{len(symbols)}] {symbol}")
                
                fetch_params = BinanceGetKlinesParams(
                    symbol=symbol,
                    interval=interval,
                    start_ts_ms=tx_data.start_timestamp_ms,
                    end_ts_ms=tx_data.end_timestamp_ms
                )
                
                klines_response = fetcher.get_klines_once(fetch_params)
                
                if klines_response.is_empty():
                    logger.warning(f"No data: {symbol}")
                    result.failed_count += 1
                    result.results.append(
                        SymbolProcessingResult(
                            symbol=symbol,
                            interval=interval,
                            success=False,
                            error_message="No data from API"
                        )
                    )
                    continue
                
                df = fetcher.klines_to_dataframe(klines_response)
                
                start_date = klines_response.start_time.strftime("%Y%m%d")
                end_date = klines_response.end_time.strftime("%Y%m%d")
                
                output_meta = DataFrameOutput(
                    symbol=symbol,
                    interval=interval,
                    row_count=len(df),
                    file_path=f"{start_date}_{end_date}",
                    compression=config.compression
                )
                
                output_meta = fetcher.save_to_parquet(df, output_meta)
                
                result.successful_count += 1
                result.results.append(
                    SymbolProcessingResult(
                        symbol=symbol,
                        interval=interval,
                        success=True,
                        klines_count=klines_response.total_count,
                        file_path=output_meta.file_path,
                        start_date=start_date,
                        end_date=end_date
                    )
                )
                
                time.sleep(1)
                
            except Exception as e:
                result.failed_count += 1
                result.results.append(
                    SymbolProcessingResult(
                        symbol=symbol,
                        interval=interval,
                        success=False,
                        error_message=str(e)
                    )
                )
                logger.error(f"Failed {symbol}: {e}")
                continue
        
        result.end_time = datetime.now()
        logger.info(result.summary())
        return result
        
    except Exception as e:
        logger = get_logger("binance_ingestion")
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        result.end_time = datetime.now()
        return result
