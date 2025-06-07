"""Funkcje pomocnicze do analizy szeregów czasowych."""

import os
import sys
import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, Union
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz


def setup_logging(log_file: Optional[str] = None, log_level: str = 'INFO') -> None:
    """Konfiguruje system logowania.
    
    Args:
        log_file: Ścieżka do pliku logu. Jeśli None, logi będą wyświetlane tylko w konsoli.
        log_level: Poziom logowania (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Konfiguracja podstawowego logowania
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[logging.StreamHandler()]
    )
    
    # Dodanie pliku logu, jeśli podano
    if log_file:
        # Utwórz katalogi, jeśli nie istnieją
        log_path = Path(log_file).parent
        log_path.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)


def save_results(
    results: Dict[str, Any], 
    output_dir: Union[str, Path], 
    prefix: str = 'arima_results'
) -> Dict[str, str]:
    """Zapisuje wyniki analizy do plików.
    
    Args:
        results: Słownik z wynikami do zapisania
        output_dir: Katalog wyjściowy
        prefix: Prefiks nazwy pliku
        
    Returns:
        Słownik ze ścieżkami do zapisanych plików
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    base_filename = f"{prefix}_{timestamp}"
    
    saved_files = {}
    
    # Zapisz metadane do pliku JSON
    metadata = {
        'timestamp': timestamp,
        'model_params': {k: v for k, v in results.items() 
                        if k not in ['forecast_plot', 'residuals_plot', 'forecast_df']}
    }
    
    metadata_file = output_dir / f"{base_filename}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=4, default=str)
    saved_files['metadata'] = str(metadata_file)
    
    # Zapisz wykresy, jeśli istnieją
    if 'forecast_plot' in results and results['forecast_plot'] is not None:
        plot_file = output_dir / f"{base_filename}_forecast.png"
        results['forecast_plot'].savefig(plot_file, bbox_inches='tight')
        plt.close(results['forecast_plot'])
        saved_files['forecast_plot'] = str(plot_file)
    
    if 'residuals_plot' in results and results['residuals_plot'] is not None:
        residuals_file = output_dir / f"{base_filename}_residuals.png"
        results['residuals_plot'].savefig(residuals_file, bbox_inches='tight')
        plt.close(results['residuals_plot'])
        saved_files['residuals_plot'] = str(residuals_file)
    
    # Zapisz dane prognozy do pliku CSV, jeśli istnieją
    if 'forecast_df' in results and results['forecast_df'] is not None:
        forecast_file = output_dir / f"{base_filename}_forecast.csv"
        results['forecast_df'].to_csv(forecast_file)
        saved_files['forecast_data'] = str(forecast_file)
    
    return saved_files


def load_config(config_file: Union[str, Path]) -> Dict[str, Any]:
    """Wczytuje konfigurację z pliku JSON.
    
    Args:
        config_file: Ścieżka do pliku konfiguracyjnego
        
    Returns:
        Słownik z konfiguracją
    """
    config_file = Path(config_file)
    if not config_file.exists():
        raise FileNotFoundError(f"Plik konfiguracyjny nie istnieje: {config_file}")
    
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    return config


def get_market_hours(
    date: Union[str, datetime, pd.Timestamp], 
    timezone: str = 'Europe/Warsaw',
    market_open_hour: int = 9,
    market_close_hour: int = 17
) -> Dict[str, datetime]:
    """Zwraca godziny otwarcia i zamknięcia rynku dla danego dnia.
    
    Args:
        date: Data (może być stringiem w formacie 'YYYY-MM-DD' lub obiektem datetime)
        timezone: Strefa czasowa
        market_open_hour: Godzina otwarcia rynku
        market_close_hour: Godzina zamknięcia rynku
        
    Returns:
        Słownik z godzinami otwarcia i zamknięcia
    """
    tz = pytz.timezone(timezone)
    
    if isinstance(date, str):
        date = pd.to_datetime(date).tz_localize(tz)
    elif isinstance(date, (datetime, pd.Timestamp)) and date.tzinfo is None:
        date = tz.localize(date)
    
    # Ustaw godzinę otwarcia i zamknięcia
    market_open = date.replace(hour=market_open_hour, minute=0, second=0, microsecond=0)
    market_close = date.replace(hour=market_close_hour, minute=0, second=0, microsecond=0)
    
    return {
        'market_open': market_open,
        'market_close': market_close,
        'market_timezone': timezone
    }


def is_market_open(
    dt: Union[str, datetime, pd.Timestamp], 
    timezone: str = 'Europe/Warsaw',
    market_open_hour: int = 9,
    market_close_hour: int = 17,
    trading_days: list = None
) -> bool:
    """Sprawdza, czy rynek jest otwarty w podanym czasie.
    
    Args:
        dt: Data i czas do sprawdzenia
        timezone: Strefa czasowa
        market_open_hour: Godzina otwarcia rynku
        market_close_hour: Godzina zamknięcia rynku
        trading_days: Lista dni tygodnia, w których rynek jest otwarty (0=poniedziałek, 6=niedziela)
        
    Returns:
        True jeśli rynek jest otwarty, w przeciwnym razie False
    """
    trading_days = trading_days or [0, 1, 2, 3, 4]  # Domyślnie od poniedziałku do piątku
    tz = pytz.timezone(timezone)
    
    if isinstance(dt, str):
        dt = pd.to_datetime(dt).tz_localize(tz)
    elif isinstance(dt, (datetime, pd.Timestamp)) and dt.tzinfo is None:
        dt = tz.localize(dt)
    
    # Sprawdź dzień tygodnia
    if dt.weekday() not in trading_days:
        return False
    
    # Sprawdź godzinę
    market_open = dt.replace(hour=market_open_hour, minute=0, second=0, microsecond=0)
    market_close = dt.replace(hour=market_close_hour, minute=0, second=0, microsecond=0)
    
    return market_open <= dt < market_close


def calculate_returns(series: pd.Series, method: str = 'log') -> pd.Series:
    """Oblicza stopy zwrotu.
    
    Args:
        series: Szereg czasowy cen
        method: Metoda obliczania stóp zwrotu ('log' lub 'simple')
        
    Returns:
        Szereg czasowy stóp zwrotu
    """
    if method == 'log':
        return np.log(series / series.shift(1)).dropna()
    elif method == 'simple':
        return (series / series.shift(1) - 1).dropna()
    else:
        raise ValueError("Nieznana metoda. Użyj 'log' lub 'simple'.")


def calculate_volatility(series: pd.Series, window: int = 21, annualize: bool = True) -> pd.Series:
    """Oblicza zmienność (odchylenie standardowe stóp zwrotu).
    
    Args:
        series: Szereg czasowy cen
        window: Okno do obliczania zmienności (w okresach)
        annualize: Czy annualizować zmienność (zakładając 252 dni handlowych)
        
    Returns:
        Szereg czasowy zmienności
    """
    returns = calculate_returns(series, method='log')
    volatility = returns.rolling(window=window).std()
    
    if annualize:
        # Annualizacja (zakładając, że dane są dzienne)
        volatility = volatility * np.sqrt(252)
    
    return volatility


def add_technical_indicators(
    df: pd.DataFrame, 
    prices: str = 'close', 
    volume: str = 'volume',
    windows: list = None
) -> pd.DataFrame:
    """Dodaje wskaźniki techniczne do ramki danych.
    
    Args:
        df: Ramka danych z danymi cenowymi
        prices: Nazwa kolumny z cenami
        volume: Nazwa kolumny z wolumenem
        windows: Lista okien dla średnich kroczących
        
    Returns:
        Ramka danych z dodanymi wskaźnikami
    """
    if windows is None:
        windows = [5, 10, 20, 50, 100, 200]
    
    df = df.copy()
    
    # Średnie kroczące
    for window in windows:
        df[f'ma{window}'] = df[prices].rolling(window=window).mean()
    
    # Wskaźnik RSI (Relative Strength Index)
    delta = df[prices].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    # Wskaźnik MACD
    exp1 = df[prices].ewm(span=12, adjust=False).mean()
    exp2 = df[prices].ewm(span=26, adjust=False).mean()
    df['macd'] = exp1 - exp2
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']
    
    # Wskaźnik Bollinger Bands
    df['bb_middle'] = df[prices].rolling(window=20).mean()
    df['bb_std'] = df[prices].rolling(window=20).std()
    df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
    df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)
    
    # Wolumen
    if volume in df.columns:
        df['volume_ma'] = df[volume].rolling(window=20).mean()
    
    return df


def print_analysis_summary(series: pd.Series, title: str = "Analiza szeregu czasowego") -> None:
    """Wyświetla podsumowanie analizy szeregu czasowego.
    
    Args:
        series: Szereg czasowy do analizy
        title: Tytuł podsumowania
    """
    print(f"\n{'='*50}")
    print(f"{title.upper()}")
    print(f"{'='*50}")
    print(f"Okres: {series.index[0]} - {series.index[-1]}")
    print(f"Liczba obserwacji: {len(series):,}")
    print(f"Wartości brakujące: {series.isnull().sum()}")
    print("\nStatystyki opisowe:")
    print(series.describe())
    
    # Test normalności
    from scipy import stats
    _, p_value = stats.normaltest(series.dropna())
    print(f"\nTest normalności (p-value): {p_value:.4f}")
    
    # Test stacjonarności (ADF)
    from statsmodels.tsa.stattools import adfuller
    result = adfuller(series.dropna())
    print(f"\nTest Dickey-Fuller (p-value): {result[1]:.4f}")
    if result[1] <= 0.05:
        print("Szereg jest stacjonarny (p <= 0.05)")
    else:
        print("Szereg NIE jest stacjonarny (p > 0.05)")
    
    # Autokorelacja
    print(f"\nAutokorelacja (lag 1): {series.autocorr():.4f}")
    
    print(f"{'='*50}\n")
