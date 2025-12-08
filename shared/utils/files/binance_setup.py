import os
from typing import Dict, Any, Optional
from pathlib import Path
import sys

# Add parent to path for imports
sys.path.append(str(Path(__file__).parent.parent))

try:
    from config.config_loader import get_config_loader
    USE_CONFIG_LOADER = True
except ImportError:
    USE_CONFIG_LOADER = False

BINANCE_BASE_URL = "https://api.binance.com"
KLINES_ENDPOINT = "/api/v3/klines"
EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
REQUESTS_PER_MINUTE = 1200
REQUEST_WEIGHT_LIMIT = 6000
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1
MAX_LIMIT_PER_REQUEST = 1000
BINANCE_KLINES_INTERVALS = {
    '1m': 60 * 1000,
    '5m': 5 * 60 * 1000,
    '15m': 15 * 60 * 1000,
    '1h': 60 * 60 * 1000,
    '4h': 4 * 60 * 60 * 1000,
    '1d': 24 * 60 * 60 * 1000
}


"""
Configuration classes for Binance API and Database connections
Now uses centralized YAML configuration
"""



class BinanceConfig:
    """Configuration for Binance API"""
    
    def __init__(self, use_yaml: bool = True):
        """
        Initialize Binance configuration
        
        Args:
            use_yaml: Whether to load from YAML config (default: True)
        """
        self.use_yaml = use_yaml and USE_CONFIG_LOADER
        
        if self.use_yaml:
            self._load_from_yaml()
        else:
            self._load_from_env()
    
    def _load_from_yaml(self):
        """Load configuration from YAML files"""
        loader = get_config_loader()
        config = loader.get_binance_config()
        
        # API settings
        api_config = config.get('api', {})
        self.base_url = api_config.get('base_url', 'https://api.binance.com')
        self.api_key = api_config.get('api_key', '')
        self.api_secret = api_config.get('api_secret', '')
        
        # Endpoints
        endpoints = api_config.get('endpoints', {})
        self.klines_endpoint = endpoints.get('klines', '/api/v3/klines')
        self.exchange_info_endpoint = endpoints.get('exchange_info', '/api/v3/exchangeInfo')
        
        # Rate limits
        rate_limits = api_config.get('rate_limits', {})
        self.requests_per_minute = rate_limits.get('requests_per_minute', 1200)
        self.request_weight_limit = rate_limits.get('request_weight_per_minute', 6000)
        
        # Retry settings
        retry_config = api_config.get('retry', {})
        self.retry_attempts = retry_config.get('max_attempts', 3)
        self.retry_delay = retry_config.get('backoff_factor', 2)
        
        # Timeout settings
        timeout_config = api_config.get('timeout', {})
        self.connect_timeout = timeout_config.get('connect', 10)
        self.read_timeout = timeout_config.get('read', 30)
        
        # Ingestion settings
        ingestion_config = config.get('ingestion', {})
        self.intervals = ingestion_config.get('interval_ms', {
            '1m': 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000
        })
        
        limits = ingestion_config.get('limits', {})
        self.max_klines_per_request = limits.get('max_klines_per_request', 1000)
        self.max_concurrent_symbols = limits.get('max_concurrent_symbols', 5)
        
        # Currency settings
        currencies = ingestion_config.get('currencies', {})
        self.exclude_currencies = set(currencies.get('exclude', []))
        self.stablecoins = set(currencies.get('stablecoins', ['USDT', 'USDC', 'BUSD']))
        
        # Storage settings
        storage = ingestion_config.get('storage', {})
        self.output_dir = storage.get('output_dir', '/app/output/raw_rates')
        self.storage_format = storage.get('format', 'parquet')
        self.compression = storage.get('compression', 'snappy')
    
    def _load_from_env(self):
        """Fallback: Load from environment variables"""
        self.base_url = os.getenv('BINANCE_API_BASE', 'https://api.binance.com')
        self.klines_endpoint = '/api/v3/klines'
        self.exchange_info_endpoint = '/api/v3/exchangeInfo'
        
        # Rate limiting
        self.requests_per_minute = 1200
        self.request_weight_limit = 6000
        self.retry_attempts = 3
        self.retry_delay = 1
        
        # Timeouts
        self.connect_timeout = 10
        self.read_timeout = 30
        
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
        self.max_concurrent_symbols = 5
        
        # Currencies
        self.exclude_currencies = {'VND', 'USD', 'EUR', 'GBP', 'JPY'}
        self.stablecoins = {'USDT', 'USDC', 'BUSD'}
        
        # Storage
        self.output_dir = '/app/output/raw_rates'
        self.storage_format = 'parquet'
        self.compression = 'snappy'
    
    def get_klines_url(self) -> str:
        """Get full URL for klines endpoint"""
        return f"{self.base_url}{self.klines_endpoint}"
    
    def get_exchange_info_url(self) -> str:
        """Get full URL for exchange info endpoint"""
        return f"{self.base_url}{self.exchange_info_endpoint}"
    
    def get_interval_ms(self, interval: str) -> int:
        """Get interval in milliseconds"""
        return self.intervals.get(interval, self.intervals['1h'])
    
    def should_exclude_currency(self, currency: str) -> bool:
        """Check if currency should be excluded"""
        return currency.upper() in self.exclude_currencies
    
    def is_stablecoin(self, currency: str) -> bool:
        """Check if currency is a stablecoin"""
        return currency.upper() in self.stablecoins
    
    def __repr__(self) -> str:
        return f"BinanceConfig(base_url={self.base_url}, use_yaml={self.use_yaml})"


class DatabaseConfig:
    """Configuration for database connections"""
    
    def __init__(self, use_yaml: bool = True):
        """
        Initialize database configuration
        
        Args:
            use_yaml: Whether to load from YAML config (default: True)
        """
        self.use_yaml = use_yaml and USE_CONFIG_LOADER
        
        if self.use_yaml:
            self._load_from_yaml()
        else:
            self._load_from_env()
    
    def _load_from_yaml(self):
        """Load configuration from YAML files"""
        loader = get_config_loader()
        config = loader.get_database_config()
        
        # PostgreSQL settings
        pg_config = config.get('postgresql', {})
        connection = pg_config.get('connection', {})
        
        self.host = connection.get('host', 'localhost')
        self.port = int(connection.get('port', 5432))
        self.user = connection.get('user', 'dataeng')
        self.password = connection.get('password', 'dataeng123')
        self.database = connection.get('database', 'dwh')
        
        # Pool settings
        pool = pg_config.get('pool', {})
        self.pool_size = pool.get('size', 10)
        self.max_overflow = pool.get('max_overflow', 20)
        self.pool_timeout = pool.get('timeout', 30)
        self.pool_recycle = pool.get('recycle', 3600)
        
        # Schema names
        schemas = pg_config.get('schemas', {})
        self.schema_raw = schemas.get('raw', 'public')
        self.schema_staging = schemas.get('staging', 'staging')
        self.schema_int = schemas.get('integration', 'int')
        self.schema_marts = schemas.get('marts', 'marts')
        self.schema_snapshots = schemas.get('snapshots', 'snapshots')
    
    def _load_from_env(self):
        """Fallback: Load from environment variables"""
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', 5432))
        self.user = os.getenv('POSTGRES_USER', 'dataeng')
        self.password = os.getenv('POSTGRES_PASSWORD', 'dataeng123')
        self.database = os.getenv('POSTGRES_DB', 'dwh')
        
        # Pool settings
        self.pool_size = 10
        self.max_overflow = 20
        self.pool_timeout = 30
        self.pool_recycle = 3600
        
        # Schema names
        self.schema_raw = 'public'
        self.schema_staging = 'staging'
        self.schema_int = 'int'
        self.schema_marts = 'marts'
        self.schema_snapshots = 'snapshots'
    
    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
    
    def get_connection_dict(self) -> Dict[str, Any]:
        """Get connection parameters as dictionary"""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database
        }
    
    def get_sqlalchemy_url(self) -> str:
        """Get SQLAlchemy connection URL"""
        return self.get_connection_string()
    
    def __repr__(self) -> str:
        return (
            f"DatabaseConfig(host={self.host}, port={self.port}, "
            f"database={self.database}, use_yaml={self.use_yaml})"
        )


class RedisConfig:
    """Configuration for Redis cache"""
    
    def __init__(self, use_yaml: bool = True):
        """
        Initialize Redis configuration
        
        Args:
            use_yaml: Whether to load from YAML config (default: True)
        """
        self.use_yaml = use_yaml and USE_CONFIG_LOADER
        
        if self.use_yaml:
            self._load_from_yaml()
        else:
            self._load_from_env()
    
    def _load_from_yaml(self):
        """Load configuration from YAML files"""
        loader = get_config_loader()
        config = loader.get_database_config()
        
        # Redis settings
        redis_config = config.get('redis', {})
        connection = redis_config.get('connection', {})
        
        self.host = connection.get('host', 'localhost')
        self.port = int(connection.get('port', 6379))
        self.db = int(connection.get('db', 0))
        self.password = connection.get('password', None)
        
        # Settings
        settings = redis_config.get('settings', {})
        self.decode_responses = settings.get('decode_responses', True)
        self.socket_timeout = settings.get('socket_timeout', 5)
        self.max_connections = settings.get('max_connections', 50)
        
        # Key prefixes
        keys = redis_config.get('keys', {})
        self.rate_limit_prefix = keys.get('rate_limit', 'binance:ratelimit:')
        self.cache_prefix = keys.get('cache', 'crypto:cache:')
        
        # TTL settings
        ttl = redis_config.get('ttl', {})
        self.default_ttl = ttl.get('default', 3600)
        self.rates_cache_ttl = ttl.get('rates_cache', 1800)
    
    def _load_from_env(self):
        """Fallback: Load from environment variables"""
        self.host = os.getenv('REDIS_HOST', 'localhost')
        self.port = int(os.getenv('REDIS_PORT', 6379))
        self.db = int(os.getenv('REDIS_DB', 0))
        self.password = os.getenv('REDIS_PASSWORD', None)
        
        self.decode_responses = True
        self.socket_timeout = 5
        self.max_connections = 50
        
        self.rate_limit_prefix = 'binance:ratelimit:'
        self.cache_prefix = 'crypto:cache:'
        
        self.default_ttl = 3600
        self.rates_cache_ttl = 1800
    
    def get_connection_dict(self) -> Dict[str, Any]:
        """Get connection parameters as dictionary"""
        params = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
            'decode_responses': self.decode_responses,
            'socket_timeout': self.socket_timeout,
            'max_connections': self.max_connections
        }
        
        if self.password:
            params['password'] = self.password
        
        return params
    
    def get_connection_url(self) -> str:
        """Get Redis connection URL"""
        auth = f":{self.password}@" if self.password else ""
        return f"redis://{auth}{self.host}:{self.port}/{self.db}"
    
    def __repr__(self) -> str:
        return (
            f"RedisConfig(host={self.host}, port={self.port}, "
            f"db={self.db}, use_yaml={self.use_yaml})"
        )