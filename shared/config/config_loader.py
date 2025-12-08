"""
Configuration Loader for Data Platform
Loads and merges YAML configuration files with environment variable substitution
"""
import os
import re
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Union


class ConfigLoader:
    """Load and manage configuration from YAML files"""
    
    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize config loader
        
        Args:
            config_dir: Path to config directory (default: /app/shared/config)
        """
        if config_dir is None:
            # Try to find config dir relative to this file
            current_file = Path(__file__)
            config_dir = current_file.parent
        
        self.config_dir = Path(config_dir)
        self.env = os.getenv('ENV', 'dev')
        self._config_cache: Dict[str, Dict[str, Any]] = {}
        
        # Validate config directory exists
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config directory not found: {self.config_dir}")
    
    def load(
        self, 
        config_name: str, 
        env_specific: bool = True,
        cache: bool = True
    ) -> Dict[str, Any]:
        """
        Load configuration file with environment variable substitution
        
        Args:
            config_name: Name of config file (e.g., 'database', 'binance')
            env_specific: Whether to load environment-specific overrides
            cache: Whether to cache the loaded config
        
        Returns:
            Merged configuration dictionary
        """
        cache_key = f"{config_name}_{self.env if env_specific else 'base'}"
        
        # Return cached config if available
        if cache and cache_key in self._config_cache:
            return self._config_cache[cache_key]
        
        # Load base config
        base_config = self._load_yaml(self.config_dir / 'base_config.yaml')
        
        # Load service-specific config
        service_file = self.config_dir / 'services' / f'{config_name}.yaml'
        service_config = self._load_yaml(service_file)
        
        if not service_config and not base_config:
            raise FileNotFoundError(
                f"Config file not found: {service_file}"
            )
        
        # Load environment-specific overrides
        env_config = {}
        if env_specific:
            env_file = self.config_dir / 'environments' / f'{self.env}.yaml'
            if env_file.exists():
                env_config = self._load_yaml(env_file)
        
        # Merge configs (env overrides service overrides base)
        merged = self._deep_merge(base_config, service_config, env_config)
        
        # Substitute environment variables
        merged = self._substitute_env_vars(merged)
        
        # Cache if requested
        if cache:
            self._config_cache[cache_key] = merged
        
        return merged
    
    def _load_yaml(self, file_path: Path) -> Dict[str, Any]:
        """
        Load YAML file
        
        Args:
            file_path: Path to YAML file
        
        Returns:
            Parsed YAML as dictionary
        """
        if not file_path.exists():
            return {}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
                return content if content is not None else {}
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {file_path}: {e}")
    
    def _deep_merge(self, *dicts) -> Dict[str, Any]:
        """
        Deep merge multiple dictionaries
        Later dicts override earlier ones
        
        Args:
            *dicts: Variable number of dictionaries to merge
        
        Returns:
            Merged dictionary
        """
        result = {}
        for d in dicts:
            if not d:
                continue
            for key, value in d.items():
                if (
                    key in result 
                    and isinstance(result[key], dict) 
                    and isinstance(value, dict)
                ):
                    result[key] = self._deep_merge(result[key], value)
                else:
                    result[key] = value
        return result
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in config
        Supports ${VAR} and ${VAR:-default} syntax
        
        Args:
            config: Config value to process
        
        Returns:
            Config with substituted values
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        
        elif isinstance(config, str):
            # Handle ${VAR:-default} syntax
            if '${' in config:
                pattern = r'\$\{([^:}]+)(?::-(.*?))?\}'
                
                def replace_var(match):
                    var_name = match.group(1)
                    default_value = match.group(2) or ''
                    return os.getenv(var_name, default_value)
                
                return re.sub(pattern, replace_var, config)
        
        return config
    
    def get(self, config_name: str, key_path: str, default: Any = None) -> Any:
        """
        Get nested configuration value using dot notation
        
        Args:
            config_name: Config file name
            key_path: Dot-separated path (e.g., 'api.base_url')
            default: Default value if key not found
        
        Returns:
            Configuration value
        
        Example:
            config.get('binance', 'api.rate_limits.requests_per_minute')
        """
        config = self.load(config_name)
        
        keys = key_path.split('.')
        value = config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.load('database')
    
    def get_binance_config(self) -> Dict[str, Any]:
        """Get Binance API configuration"""
        return self.load('binance')
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.load('logging')
    
    def reload(self, config_name: Optional[str] = None):
        """
        Reload configuration(s) from disk
        
        Args:
            config_name: Specific config to reload, or None to reload all
        """
        if config_name:
            # Remove specific config from cache
            keys_to_remove = [
                k for k in self._config_cache.keys() 
                if k.startswith(config_name)
            ]
            for key in keys_to_remove:
                del self._config_cache[key]
        else:
            # Clear entire cache
            self._config_cache.clear()
    
    def validate_config(self, config_name: str) -> bool:
        """
        Validate that a config file exists and is valid YAML
        
        Args:
            config_name: Name of config to validate
        
        Returns:
            True if valid, False otherwise
        """
        try:
            config = self.load(config_name, cache=False)
            return bool(config)
        except (FileNotFoundError, ValueError):
            return False
    
    def __repr__(self) -> str:
        return (
            f"ConfigLoader(config_dir={self.config_dir}, "
            f"env={self.env}, "
            f"cached_configs={len(self._config_cache)})"
        )


# Singleton instance
_config_loader: Optional[ConfigLoader] = None


def get_config_loader(config_dir: Optional[str] = None) -> ConfigLoader:
    """
    Get or create config loader singleton
    
    Args:
        config_dir: Optional config directory path
    
    Returns:
        ConfigLoader instance
    """
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader(config_dir)
    return _config_loader


def load_config(config_name: str, **kwargs) -> Dict[str, Any]:
    """
    Shortcut function to load config
    
    Args:
        config_name: Name of config file
        **kwargs: Additional arguments for ConfigLoader.load()
    
    Returns:
        Configuration dictionary
    """
    loader = get_config_loader()
    return loader.load(config_name, **kwargs)


# Convenience functions
def get_database_config() -> Dict[str, Any]:
    """Get database configuration"""
    return get_config_loader().get_database_config()


def get_binance_config() -> Dict[str, Any]:
    """Get Binance configuration"""
    return get_config_loader().get_binance_config()


def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration"""
    return get_config_loader().get_logging_config()