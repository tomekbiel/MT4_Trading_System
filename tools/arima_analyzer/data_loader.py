"""Moduł do ładowania i przygotowywania danych do analizy."""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Optional, Tuple, Dict, Any, Union, List
from datetime import datetime, time, timedelta
import pytz

from .config import DATA_DIR, DEFAULT_SYMBOL, DEFAULT_TIMEFRAME, DEFAULT_COLUMN, MARKET_OPEN, MARKET_CLOSE, TRADING_DAYS

logger = logging.getLogger(__name__)


class DataLoader:
    """Klasa do ładowania i przygotowywania danych finansowych."""
    
    def __init__(self, data_dir: Path = None):
        """Inicjalizacja DataLoader.
        
        Args:
            data_dir: Katalog z danymi historycznymi. Domyślnie używany z config.py
        """
        self.data_dir = data_dir if data_dir else DATA_DIR
        self.timezone = pytz.timezone('Europe/Warsaw')
        logger.info(f"Zainicjalizowano DataLoader z katalogiem danych: {self.data_dir}")
    
    def load_data(
        self, 
        symbol: str = None, 
        timeframe: str = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None
    ) -> pd.DataFrame:
        """Wczytuje dane historyczne dla danego symbolu i interwału.
        
        Args:
            symbol: Symbol instrumentu (np. 'US.100+')
            timeframe: Interwał czasowy (np. 'M1', 'H1', 'D1')
            start_date: Data początkowa (włącznie)
            end_date: Data końcowa (włącznie)
            
        Returns:
            DataFrame z danymi
        """
        symbol = symbol or DEFAULT_SYMBOL
        timeframe = timeframe or DEFAULT_TIMEFRAME
        
        # Buduj ścieżkę do pliku
        file_path = self.data_dir / symbol / timeframe / f"{symbol}_{timeframe}.csv"
        
        if not file_path.exists():
            # Spróbuj alternatywnej nazwy pliku
            file_path = self.data_dir / symbol / timeframe / f"{symbol}1.csv"
            if not file_path.exists():
                raise FileNotFoundError(f"Nie znaleziono pliku z danymi: {file_path}")
        
        logger.info(f"Wczytywanie danych z {file_path}")
        
        try:
            # Wczytaj dane
            df = pd.read_csv(
                file_path,
                parse_dates=['time'],
                dayfirst=True
            )
            
            # Ustaw indeks czasowy
            if 'time' in df.columns:
                df.set_index('time', inplace=True)
            
            # Konwersja indeksu na czas polski
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC').tz_convert(self.timezone)
            
            # Filtruj po dacie
            if start_date:
                if isinstance(start_date, str):
                    start_date = pd.to_datetime(start_date, dayfirst=True)
                start_date = self.timezone.localize(start_date) if start_date.tzinfo is None else start_date
                df = df[df.index >= start_date]
                
            if end_date:
                if isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date, dayfirst=True)
                end_date = self.timezone.localize(end_date) if end_date.tzinfo is None else end_date
                # Uwzględnij cały dzień końcowy
                end_date = end_date.replace(hour=23, minute=59, second=59)
                df = df[df.index <= end_date]
            
            logger.info(f"Zaimportowano {len(df)} rekordów")
            return df
            
        except Exception as e:
            logger.error(f"Błąd podczas wczytywania danych: {e}")
            raise
    
    def filter_trading_hours(
        self, 
        df: pd.DataFrame, 
        market_open: int = None, 
        market_close: int = None,
        trading_days: List[int] = None
    ) -> pd.DataFrame:
        """Filtruje dane uwzględniając tylko godziny handlu.
        
        Args:
            df: DataFrame z danymi
            market_open: Godzina otwarcia rynku (czas polski)
            market_close: Godzina zamknięcia rynku (czas polski)
            trading_days: Lista dni tygodnia, w których odbywa się handel (0=poniedziałek, 6=niedziela)
            
        Returns:
            Przefiltrowany DataFrame
        """
        market_open = market_open or MARKET_OPEN
        market_close = market_close or MARKET_CLOSE
        trading_days = trading_days or TRADING_DAYS
        
        # Filtruj dni tygodnia
        if trading_days is not None:
            df = df[df.index.dayofweek.isin(trading_days)]
        
        # Filtruj godziny handlu
        df = df.between_time(
            time(market_open, 0), 
            time(market_close - 1, 59),  # -1 bo between_time jest włącznie z końcem
            include_end=True
        )
        
        return df
    
    def prepare_data(
        self,
        symbol: str = None,
        timeframe: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        column: str = None,
        filter_trading: bool = True
    ) -> Tuple[pd.Series, Dict[str, Any]]:
        """Przygotowuje dane do analizy.
        
        Args:
            symbol: Symbol instrumentu
            timeframe: Interwał czasowy
            start_date: Data początkowa
            end_date: Data końcowa
            column: Kolumna do analizy (domyślnie 'close')
            filter_trading: Czy filtrować godziny handlu
            
        Returns:
            Krotka (szereg czasowy, metadane)
        """
        column = column or DEFAULT_COLUMN
        
        # Wczytaj dane
        df = self.load_data(symbol, timeframe, start_date, end_date)
        
        # Filtruj godziny handlu jeśli wymagane
        if filter_trading and timeframe != 'D1':  # Dla D1 nie filtrujemy godzin
            df = self.filter_trading_hours(df)
        
        # Sprawdź czy kolumna istnieje
        if column not in df.columns:
            raise ValueError(f"Kolumna '{column}' nie istnieje w danych. Dostępne kolumny: {df.columns.tolist()}")
        
        # Pobierz serię czasową
        series = df[column].copy()
        
        # Przygotuj metadane
        metadata = {
            'symbol': symbol,
            'timeframe': timeframe,
            'start_date': series.index.min(),
            'end_date': series.index.max(),
            'length': len(series),
            'column': column
        }
        
        return series, metadata


def example_usage():
    """Przykładowe użycie klasy DataLoader."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    loader = DataLoader()
    
    # Wczytaj dane dla US.100+ M1 z ostatnich 5 dni
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)
    
    try:
        # Wczytaj dane bez filtrowania godzin handlu
        series, metadata = loader.prepare_data(
            symbol='US.100+',
            timeframe='M1',
            start_date=start_date,
            end_date=end_date,
            filter_trading=False
        )
        print(f"\nPrzykładowe dane (wszystkie godziny):")
        print(series.head())
        print(f"Liczba obserwacji: {len(series)}")
        
        # Wczytaj dane z filtrowaniem godzin handlu
        series_filtered, _ = loader.prepare_data(
            symbol='US.100+',
            timeframe='M1',
            start_date=start_date,
            end_date=end_date,
            filter_trading=True
        )
        print(f"\nPrzykładowe dane (tylko godziny handlu):")
        print(series_filtered.head())
        print(f"Liczba obserwacji: {len(series_filtered)}")
        
    except Exception as e:
        print(f"Wystąpił błąd: {e}")


if __name__ == "__main__":
    example_usage()
