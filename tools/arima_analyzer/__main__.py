#!/usr/bin/env python3
"""
Główny moduł do analizy szeregów czasowych z wykorzystaniem modeli ARIMA/SARIMA.

Użycie z wiersza poleceń:
    python -m arima_analyzer --symbol US.100+ --timeframe M1 --train-size 0.8 --output-dir results

Lub z kodu Pythona:
    from arima_analyzer import run_analysis
    results = run_analysis(symbol='US.100+', timeframe='M1')
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Dodaj katalog główny projektu do ścieżki Pythona
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tools.arima_analyzer.data_loader import DataLoader
from tools.arima_analyzer.seasonal_decompose import SeasonalAnalyzer
from tools.arima_analyzer.arima_model import ARIMAAnalyzer, train_test_split
from tools.arima_analyzer.utils import setup_logging, save_results, print_analysis_summary


def parse_args():
    """Parsuje argumenty wiersza poleceń."""
    parser = argparse.ArgumentParser(description='Analiza szeregów czasowych z wykorzystaniem modeli ARIMA/SARIMA')
    
    # Wymagane argumenty
    parser.add_argument('--symbol', type=str, default='US.100+',
                       help='Symbol instrumentu (np. US.100+, EURUSD+, GOLDs+)')
    parser.add_argument('--timeframe', type=str, default='M1',
                       help='Interwał czasowy (M1, M5, M15, H1, H4, D1)')
    
    # Opcjonalne argumenty
    parser.add_argument('--start-date', type=str, default=None,
                       help='Data początkowa (format: YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None,
                       help='Data końcowa (format: YYYY-MM-DD)')
    parser.add_argument('--train-size', type=float, default=0.8,
                       help='Proporcja danych treningowych (domyślnie: 0.8)')
    parser.add_argument('--output-dir', type=str, default='results',
                       help='Katalog do zapisu wyników (domyślnie: results/)')
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Poziom logowania (domyślnie: INFO)')
    parser.add_argument('--no-seasonal', action='store_true',
                       help='Wyłącz modelowanie sezonowości')
    
    return parser.parse_args()


def prepare_data(symbol: str, timeframe: str, start_date: str = None, end_date: str = None) -> tuple:
    """Przygotowuje dane do analizy.
    
    Args:
        symbol: Symbol instrumentu
        timeframe: Interwał czasowy
        start_date: Data początkowa (opcjonalna)
        end_date: Data końcowa (opcjonalna)
        
    Returns:
        Krotka (series, metadata)
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Przygotowywanie danych dla {symbol} {timeframe}")
    
    # Wczytaj dane
    loader = DataLoader()
    series, metadata = loader.prepare_data(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        filter_trading=True
    )
    
    # Wyświetl podsumowanie danych
    print_analysis_summary(series, f"Analiza danych: {symbol} {timeframe}")
    
    return series, metadata


def analyze_seasonality(series: pd.Series, timeframe: str) -> dict:
    """Przeprowadza analizę sezonowości.
    
    Args:
        series: Szereg czasowy do analizy
        timeframe: Interwał czasowy (do określenia okresu sezonowości)
        
    Returns:
        Słownik z wynikami analizy sezonowości
    """
    logger = logging.getLogger(__name__)
    logger.info("Przeprowadzanie analizy sezonowości...")
    
    # Określ okres sezonowości na podstawie interwału czasowego
    seasonal_periods = {
        'M1': 96,   # 8 godzin * 60 minut / 5 (dla M1 używamy 96 jako przybliżenie)
        'M5': 96,   # 8 godzin * 12 świec na godzinę
        'M15': 32,  # 8 godzin * 4 świece na godzinę
        'H1': 8,    # 8 godzin handlowych
        'H4': 2,    # 2 świece na dzień (8h/4h)
        'D1': 5     # 5 dni handlowych w tygodniu
    }
    
    m = seasonal_periods.get(timeframe, 1)  # Domyślnie bez sezonowości
    
    # Analiza sezonowości
    analyzer = SeasonalAnalyzer(period=m)
    
    # Test stacjonarności
    stationarity = analyzer.test_stationarity(series)
    
    # Dekompozycja sezonowa
    try:
        decomposition = analyzer.decompose(series)
        
        # Wykres dekompozycji
        plt.figure(figsize=(12, 8))
        analyzer.plot_decomposition(decomposition)
        plt.suptitle(f'Dekompozycja sezonowa: {series.name}')
        plt.tight_layout()
        decomposition_plot = plt.gcf()
        plt.close()
        
        # Analiza ACF/PACF
        plt.figure(figsize=(12, 6))
        analyzer.plot_acf_pacf(series, lags=min(40, len(series)//4))
        plt.suptitle(f'ACF/PACF: {series.name}')
        plt.tight_layout()
        acf_plot = plt.gcf()
        plt.close()
        
        return {
            'is_stationary': stationarity.get('p-value', 1) <= 0.05,
            'p_value': stationarity.get('p-value', 1),
            'decomposition': decomposition,
            'decomposition_plot': decomposition_plot,
            'acf_pacf_plot': acf_plot,
            'seasonal_period': m
        }
        
    except Exception as e:
        logger.error(f"Błąd podczas analizy sezonowości: {e}")
        return {
            'is_stationary': stationarity.get('p-value', 1) <= 0.05,
            'p_value': stationarity.get('p-value', 1),
            'error': str(e)
        }


def fit_arima_model(
    train: pd.Series, 
    test: pd.Series, 
    seasonal: bool = True, 
    m: int = None
) -> dict:
    """Dopasowuje model ARIMA/SARIMA do danych.
    
    Args:
        train: Dane treningowe
        test: Dane testowe
        seasonal: Czy uwzględniać sezonowość
        m: Okres sezonowości
        
    Returns:
        Słownik z wynikami modelowania
    """
    logger = logging.getLogger(__name__)
    logger.info("Dopasowywanie modelu ARIMA...")
    
    # Inicjalizacja modelu
    model = ARIMAAnalyzer(seasonal=seasonal, m=m)
    
    try:
        # Automatyczne znajdowanie najlepszych parametrów
        best_params = model.find_best_arima(
            train,
            seasonal=seasonal,
            m=m,
            trace=True,
            error_action='ignore',
            suppress_warnings=True,
            stepwise=True
        )
        
        # Dopasowanie modelu z najlepszymi parametrami
        model_fit = model.fit(train)
        
        # Prognoza
        forecast_steps = len(test)
        forecast_result = model.forecast(steps=forecast_steps)
        
        # Ocena modelu
        metrics = model.evaluate(test, forecast_result['forecast'])
        
        # Wykres prognozy
        forecast_plot = model.plot_forecast(
            train=train,
            test=test,
            forecast=forecast_result['forecast'],
            conf_int=forecast_result.get('conf_int')
        )
        
        # Wykres reszt
        residuals = model.model_fit.resid
        plt.figure(figsize=(12, 6))
        plt.plot(residuals)
        plt.title('Reszty modelu')
        plt.grid(True)
        residuals_plot = plt.gcf()
        plt.close()
        
        # Przygotuj dane prognozy do zapisu
        forecast_df = pd.DataFrame({
            'actual': test,
            'forecast': forecast_result['forecast']
        })
        
        if 'conf_int' in forecast_result:
            forecast_df['forecast_lower'] = forecast_result['conf_int'][:, 0]
            forecast_df['forecast_upper'] = forecast_result['conf_int'][:, 1]
        
        return {
            'model': model,
            'best_params': best_params,
            'metrics': metrics,
            'forecast_plot': forecast_plot,
            'residuals_plot': residuals_plot,
            'forecast_df': forecast_df,
            'residuals': residuals
        }
        
    except Exception as e:
        logger.error(f"Błąd podczas dopasowywania modelu ARIMA: {e}")
        raise


def run_analysis(
    symbol: str,
    timeframe: str,
    start_date: str = None,
    end_date: str = None,
    train_size: float = 0.8,
    output_dir: str = 'results',
    log_level: str = 'INFO',
    seasonal: bool = True
) -> dict:
    """Przeprowadza kompleksową analizę szeregu czasowego.
    
    Args:
        symbol: Symbol instrumentu
        timeframe: Interwał czasowy
        start_date: Data początkowa (opcjonalna)
        end_date: Data końcowa (opcjonalna)
        train_size: Proporcja danych treningowych
        output_dir: Katalog do zapisu wyników
        log_level: Poziom logowania
        seasonal: Czy uwzględniać sezonowość
        
    Returns:
        Słownik z wynikami analizy
    """
    # Konfiguracja logowania
    os.makedirs(output_dir, exist_ok=True)
    log_file = os.path.join(output_dir, 'arima_analysis.log')
    setup_logging(log_file=log_file, log_level=log_level)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Rozpoczęcie analizy dla {symbol} {timeframe}")
    
    try:
        # 1. Przygotowanie danych
        series, metadata = prepare_data(symbol, timeframe, start_date, end_date)
        
        # 2. Analiza sezonowości
        seasonal_analysis = analyze_seasonality(series, timeframe)
        
        # 3. Podział na zbiór treningowy i testowy
        train, test = train_test_split(series, test_size=1-train_size)
        
        # 4. Dopasowanie modelu ARIMA
        m = seasonal_analysis.get('seasonal_period') if seasonal else None
        model_results = fit_arima_model(train, test, seasonal=seasonal, m=m)
        
        # 5. Przygotowanie wyników
        results = {
            'symbol': symbol,
            'timeframe': timeframe,
            'start_date': series.index[0],
            'end_date': series.index[-1],
            'train_size': len(train),
            'test_size': len(test),
            'model_params': model_results['best_params'],
            'metrics': model_results['metrics'],
            'forecast_plot': model_results['forecast_plot'],
            'residuals_plot': model_results['residuals_plot'],
            'forecast_df': model_results['forecast_df'],
            'seasonal_analysis': {
                'is_stationary': seasonal_analysis['is_stationary'],
                'p_value': seasonal_analysis['p_value']
            },
            'metadata': metadata
        }
        
        # 6. Zapis wyników
        saved_files = save_results(results, output_dir, prefix=f"{symbol}_{timeframe}")
        results['saved_files'] = saved_files
        
        logger.info(f"Analiza zakończona pomyślnie. Wyniki zapisano w: {output_dir}")
        
        return results
        
    except Exception as e:
        logger.exception("Wystąpił błąd podczas analizy")
        raise


def main():
    """Główna funkcja uruchamiana z wiersza poleceń."""
    args = parse_args()
    
    try:
        run_analysis(
            symbol=args.symbol,
            timeframe=args.timeframe,
            start_date=args.start_date,
            end_date=args.end_date,
            train_size=args.train_size,
            output_dir=args.output_dir,
            log_level=args.log_level,
            seasonal=not args.no_seasonal
        )
    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika.")
        sys.exit(1)
    except Exception as e:
        print(f"\nWystąpił błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
