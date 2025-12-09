
from dataclasses import dataclass

@dataclass
class BinanceGetKlinesParams:
    symbol: str
    interval: str
    start_ts_ms: int
    end_ts_ms: int

@dataclass
class BinanceGetKlinesResponse:
    symbol: str
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    fetched_at: str
