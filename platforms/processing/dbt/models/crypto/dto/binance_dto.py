from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

# -----------------------------
# KLINE FETCH PARAMETERS
# -----------------------------
@dataclass
class BinanceGetKlinesParams:
    symbol: str
    interval: str
    start_ts_ms: int
    end_ts_ms: int

    def validate(self):
        if not self.symbol:
            raise ValueError("Symbol is required")
        if self.start_ts_ms >= self.end_ts_ms:
            raise ValueError("Start timestamp must be less than end timestamp")
        return True


# -----------------------------
# CONFIG FOR INGESTION
# -----------------------------
@dataclass
class IngestionConfig:
    csv_path: str
    interval: str = "1h"
    output_dir: Optional[str] = None

    def validate(self):
        if not self.csv_path:
            raise ValueError("csv_path is required")
        return True


# -----------------------------
# TRANSACTION SUMMARY
# -----------------------------
@dataclass
class TransactionData:
    currencies: List[str]
    start_timestamp_ms: int
    end_timestamp_ms: int
    total_records: int


# -----------------------------
# RESULT FOR EACH SYMBOL
# -----------------------------
@dataclass
class SymbolProcessingResult:
    symbol: str
    interval: str
    success: bool
    klines_count: int = 0
    file_path: Optional[str] = None
    error_message: Optional[str] = None


# -----------------------------
# GENERAL INGESTION RESULT
# -----------------------------
@dataclass
class IngestionResult:
    total_symbols: int
    successful_count: int
    failed_count: int
    results: List[SymbolProcessingResult] = field(default_factory=list)

    def summary(self):
        return {
            "total_symbols": self.total_symbols,
            "success": self.successful_count,
            "failed": self.failed_count,
        }


# -----------------------------
# KLINES API RESPONSE WRAPPER
# -----------------------------
@dataclass
class KlinesResponse:
    symbol: str
    interval: str
    klines: List[List]
    total_count: int

    def is_empty(self):
        return len(self.klines) == 0


# -----------------------------
# OUTPUT METADATA
# -----------------------------
@dataclass
class DataFrameOutput:
    symbol: str
    interval: str
    file_path: Optional[str] = None
    compression: str = "snappy"
    size_bytes: Optional[int] = None
