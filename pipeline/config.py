"""Configuration module for the Hansel Attribution Pipeline."""

import configparser
from dataclasses import dataclass


@dataclass
class PipelineConfig:
    """Configuration class for the Hansel Attribution Pipeline."""
    
    db_name: str
    api_key: str
    conv_type_id: str
    max_journeys_per_request: int
    max_sessions_per_request: int
    
    @classmethod
    def from_ini(cls, config_path="config.ini"):
        """Load configuration from an INI file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            PipelineConfig: Configuration object
        """
        config = configparser.ConfigParser()
        config.read(config_path)
        
        return cls(
            db_name=config['database']['db_name'],
            api_key=config['api']['api_key'],
            conv_type_id=config['api']['conv_type_id'],
            max_journeys_per_request=int(config['api']['max_journeys_per_request']),
            max_sessions_per_request=int(config['api']['max_sessions_per_request'])
        )


def get_config(config_path="config.ini"):
    """Get pipeline configuration.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        PipelineConfig: Configuration object
    """
    return PipelineConfig.from_ini(config_path)