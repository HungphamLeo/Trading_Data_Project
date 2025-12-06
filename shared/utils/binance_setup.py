import os
from typing import Dict, Any


class BinanceConfig:
    """Configuration for Binance API"""
    
    def __init__(self):
        self.base_url = os.getenv('BINANCE_API_BASE', 'https://api.binance.com')
        self.klines_endpoint = '/api/v3/klines'
        self.exchange_info_endpoint = '/api/v3/exchangeInfo'
        
        # Rate limiting
        self.requests_per_minute = 1200
        self.request_weight_limit = 6000
        self.retry_attempts = 3
        self.retry_delay = 1  # seconds
        
        # Intervals
        self.intervals = {
            '1m': 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000
        }
        
        # Max limits
        self.max_klines_per_request = 1000
        
    def get_klines_url(self) -> str:
        return f"{self.base_url}{self.klines_endpoint}"
    
    def get_exchange_info_url(self) -> str:
        return f"{self.base_url}{self.exchange_info_endpoint}"
    
    def get_interval_ms(self, interval: str) -> int:
        return self.intervals.get(interval, self.intervals['1h'])


class DatabaseConfig:
    """Configuration for database connections"""
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', 5432))
        self.user = os.getenv('POSTGRES_USER', 'dataeng')
        self.password = os.getenv('POSTGRES_PASSWORD', 'dataeng123')
        self.database = os.getenv('POSTGRES_DB', 'dwh')
    
    def get_connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_connection_dict(self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database
        }


class RedisConfig:
    """Configuration for Redis cache"""
    
    def __init__(self):
        self.host = os.getenv('REDIS_HOST', 'localhost')
        self.port = int(os.getenv('REDIS_PORT', 6379))
        self.db = int(os.getenv('REDIS_DB', 0))
        self.decode_responses = True
    
    def get_connection_dict(self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'decode_responses': self.decode_responses
        }