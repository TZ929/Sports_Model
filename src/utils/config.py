"""
Configuration management utilities for the NBA/WNBA predictive model.
"""

import yaml
from pathlib import Path
from typing import Dict, Any


class Config:
    """Configuration manager for the sports model project."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize configuration from YAML file.
        
        Args:
            config_path: Path to the configuration YAML file
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Returns:
            Dictionary containing configuration settings
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
        with open(self.config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            
        return config
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation like 'database.type')
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_database_url(self) -> str:
        """Get database connection URL.
        
        Returns:
            Database connection string
        """
        db_type = self.get('database.type', 'sqlite')
        
        if db_type == 'sqlite':
            sqlite_path = self.get('database.sqlite_path', 'data/sports_model.db')
            return f"sqlite:///{sqlite_path}"
        elif db_type == 'postgresql':
            host = self.get('database.postgresql.host', 'localhost')
            port = self.get('database.postgresql.port', 5432)
            database = self.get('database.postgresql.database', 'sports_model')
            username = self.get('database.postgresql.username', '')
            password = self.get('database.postgresql.password', '')
            
            if username and password:
                return f"postgresql://{username}:{password}@{host}:{port}/{database}"
            else:
                return f"postgresql://{host}:{port}/{database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def get_data_path(self, path_type: str) -> Path:
        """Get data path for specified type.
        
        Args:
            path_type: Type of data path ('raw', 'processed', 'models')
            
        Returns:
            Path object for the specified data directory
        """
        base_path = Path(self.get(f'paths.data_{path_type}', f'data/{path_type}'))
        base_path.mkdir(parents=True, exist_ok=True)
        return base_path
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration.
        
        Returns:
            Dictionary with logging settings
        """
        return {
            'level': self.get('logging.level', 'INFO'),
            'format': self.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            'file': self.get('logging.file', 'logs/sports_model.log')
        }


# Global configuration instance
config = Config() 