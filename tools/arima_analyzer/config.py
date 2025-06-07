"""Konfiguracja dla modułu analizy ARIMA."""

from pathlib import Path
import os

# Ścieżki
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data' / 'historical'
RESULTS_DIR = PROJECT_ROOT / 'results'

# Ustawienia domyślne
DEFAULT_SYMBOL = 'US.100+'
DEFAULT_TIMEFRAME = 'M1'
DEFAULT_COLUMN = 'close'  # Domyślna kolumna do analizy

# Ustawienia modelu ARIMA
DEFAULT_TRAIN_SIZE = 0.8  # 80% danych do trenowania, 20% do testów
DEFAULT_ARIMA_ORDER = (1, 1, 1)  # Domyślne parametry (p,d,q)

# Godziny handlu (czas polski)
MARKET_OPEN = 9  # 9:00 czasu polskiego
MARKET_CLOSE = 17  # 17:00 czasu polskiego

# Dni tygodnia (0 = poniedziałek, 6 = niedziela)
TRADING_DAYS = [0, 1, 2, 3, 4]  # Od poniedziałku do piątku

# Konfiguracja logowania
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': 'INFO',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': os.path.join(PROJECT_ROOT, 'arima_analysis.log'),
            'formatter': 'standard',
            'level': 'DEBUG',
        },
    },
    'loggers': {
        'arima_analyzer': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': True
        }
    }
}
