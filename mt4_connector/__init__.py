# -*- coding: utf-8 -*-
"""
MT4 Connector Package - Python interface for MetaTrader 4 via ZeroMQ

This package provides a bridge between Python applications and MetaTrader 4 platform
using ZeroMQ for communication. It enables real-time market data streaming,
historical data retrieval, and trading operations.

Main Components:
- MT4BaseConnector: Base connection handling and communication
- MT4MarketDataHandler: Real-time market data operations
- MT4HistoricalDataHandler: Historical price data retrieval
- MT4CommandSender: Trading and command execution interface

Configuration is provided via DEFAULT_CONFIG dictionary which includes:
- Network ports for ZMQ communication
- Network settings (host, protocol, timeouts)
- Logging configuration
"""
import logging
from .base_connector import MT4BaseConnector
from .market_data_handler import MT4MarketDataHandler
from .historical_data_handler import MT4HistoricalDataHandler
from .command_sender import MT4CommandSender

__version__ = '1.1.0'
__author__ = 'tomekbiel.office@gmail.com'
__license__ = 'BSD 3-Clause'


# Default configuration settings for the MT4 connector
DEFAULT_CONFIG = {
    'PORTS': {
        'push': 5565,  # Python PULL, MT4 PUSH - for receiving data from MT4
        'pull': 5566,  # Python PUSH, MT4 PULL - for sending commands to MT4
        'sub': 5557    # Subscription port for market data
    },
    'NETWORK': {
        'host': 'localhost',
        'protocol': 'tcp',
        'timeout': 10000,  # Connection timeout in milliseconds
        'retries': 5,     # Number of connection retry attempts
        'retry_delay': 2.0,    # Delay between retries in seconds
        'sleep_delay': 0.25    # General operation delay in seconds
    },
    'LOGGING': {
        'level': logging.INFO,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }
}

# Export public interface
__all__ = [
    'MT4BaseConnector',
    'MT4MarketDataHandler',
    'MT4HistoricalDataHandler',
    'MT4CommandSender',
    'DEFAULT_CONFIG'
]

# Initialize logging configuration
logging.basicConfig(
    level=DEFAULT_CONFIG['LOGGING']['level'],
    format=DEFAULT_CONFIG['LOGGING']['format']
)
logger = logging.getLogger(__name__)
logger.info(f'MT4 Connector v{__version__} initialized')

def get_config():
    """
    Return a deep copy of default configuration.
    
    Returns:
        dict: A new copy of the DEFAULT_CONFIG dictionary to prevent modification
              of the original configuration.
    """
    import copy
    return copy.deepcopy(DEFAULT_CONFIG)