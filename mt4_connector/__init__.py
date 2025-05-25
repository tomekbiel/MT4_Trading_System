# -*- coding: utf-8 -*-
"""
MT4 Connector Package - Python interface for MetaTrader 4 via ZeroMQ
"""
import logging
from .base_connector import MT4BaseConnector
from .market_data_handler import MT4MarketDataHandler
from .historical_data_handler import MT4HistoricalDataHandler
from .command_sender import MT4CommandSender  # 🆕 Dodane!

# Metadane i konfiguracja pakietu
__version__ = '1.1.0'
__author__ = 'tomekbiel.office@gmail.com'
__license__ = 'BSD 3-Clause'

# ✅ Poprawiona kolejność portów zgodna z MQL4:
# MT4:  PULL (port 5555) ← Python PUSH
# MT4:  PUSH (port 5556) → Python PULL
# MT4:  PUB (port 5557) → Python SUB
DEFAULT_CONFIG = {
    'PORTS': {
        'push': 5565,  # 🔁 Python będzie PULL, MT4 PUSH
        'pull': 5566,  # 🔁 Python będzie PUSH, MT4 PULL
        'sub': 5557
    },
    'NETWORK': {
        'host': 'localhost',
        'protocol': 'tcp',
        'timeout': 10000,  # ms
        'retries': 5,
        'retry_delay': 2.0,
        'sleep_delay': 0.25  # seconds
    },
    'LOGGING': {
        'level': logging.INFO,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    }
}

__all__ = [
    'MT4BaseConnector',
    'MT4MarketDataHandler',
    'MT4HistoricalDataHandler',
    'MT4CommandSender',  # 🆕 Dodane!
    'DEFAULT_CONFIG'
]

# Inicjalizacja logowania
logging.basicConfig(
    level=DEFAULT_CONFIG['LOGGING']['level'],
    format=DEFAULT_CONFIG['LOGGING']['format']
)
logger = logging.getLogger(__name__)
logger.info(f'MT4 Connector v{__version__} initialized')

def get_config():
    """Return deep copy of default configuration"""
    import copy
    return copy.deepcopy(DEFAULT_CONFIG)
