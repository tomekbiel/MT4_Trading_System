"""
Skrypt testowy dla modułu arima_analyzer.

Uruchomienie:
    python test_arima_analyzer.py
"""

import sys
import os
from pathlib import Path

# Dodaj katalog główny projektu do ścieżki Pythona
sys.path.insert(0, str(Path(__file__).parent))

from tools.arima_analyzer import run_analysis

def main():
    """Główna funkcja testowa."""
    print("Testowanie modułu arima_analyzer...")
    
    # Ustawienia testu
    symbol = 'US.100+'
    timeframe = 'M1'
    output_dir = 'test_results'
    
    try:
        # Uruchom analizę na małym podzbiorze danych
        print(f"Przeprowadzam analizę dla {symbol} {timeframe}...")
        results = run_analysis(
            symbol=symbol,
            timeframe=timeframe,
            train_size=0.8,
            output_dir=output_dir,
            log_level='INFO',
            seasonal=True
        )
        
        # Wyświetl podsumowanie wyników
        print("\nTest zakończony pomyślnie!")
        print(f"Wyniki zapisano w katalogu: {output_dir}")
        print("\nMetryki jakości prognozy:")
        for metric, value in results['metrics'].items():
            print(f"  {metric.upper()}: {value:.4f}")
            
    except Exception as e:
        print(f"\nWystąpił błąd podczas testu: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
