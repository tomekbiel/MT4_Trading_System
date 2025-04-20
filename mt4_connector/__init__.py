# -*- coding: utf-8 -*-
"""
MT4 Connector Package - Python interface for MetaTrader 4 via ZeroMQ

Exposes main classes for easy import:
from mt4_connector import MT4BaseConnector, MT4MarketDataHandler,
                         MT4HistoricalDataHandler, MT4TradeHandler
"""

# 1. Importy klas dostępnych bezpośrednio z pakietu
from .base_connector import MT4BaseConnector
from .market_data_handler import MT4MarketDataHandler
from .historical_data_handler import MT4HistoricalDataHandler
from .trade_handler import MT4TradeHandler

# 2. Metadane pakietu
__version__ = '1.0.0'
__author__ = 'tomekbiel.office@gmail.com'
__license__ = 'BSD 3-Clause'

# 3. Lista symboli dostępnych publicznie
__all__ = [
    'MT4BaseConnector',
    'MT4MarketDataHandler',
    'MT4HistoricalDataHandler',
    'MT4TradeHandler'
]

# 4. Inicjalizacja logowania (opcjonalne)
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f'MT4 Connector v{__version__} initialized')

# Dodaj do __init__.py:

# A. Stałe konfiguracyjne
DEFAULT_PORTS = {
    'push': 5555,
    'pull': 5556,
    'sub': 5557
}

# B. Funkcję pomocniczą
def get_version():
    """Return package version"""
    return __version__

"""
🛠 Jak to dodać w PyCharm:
Kliknij prawym przyciskiem na folder mt4_connector w oknie projektu

Wybierz New → Python File

Nazwij plik __init__.py (ważne: podwójne podkreślenia)

Wklej powyższą zawartość

📌 Dlaczego te elementy są ważne?
Importy klas
Pozwalają na czysty import w stylu:

python
from mt4_connector import MT4MarketDataHandler
zamiast

python
from mt4_connector.market_data_handler import MT4MarketDataHandler
Metadane
Wersjonowanie i informacje o autorze przydają się przy publikacji pakietu.

__all__
Kontroluje jakie obiekty są eksportowane gdy ktoś użyje:

python
from mt4_connector import *
Logowanie
Ułatwia debugowanie (możesz później zmienić poziom na DEBUG dla szczegółowych logów).
"""