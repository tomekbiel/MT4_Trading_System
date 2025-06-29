import os
import pandas as pd
from datetime import datetime

# Konfiguracja
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'historical'))
TIMEFRAME = 'M5'  # Ustawiamy na M5

def get_last_quotes(filepath, num_quotes=5):
    """Pobiera ostatnie N notowań z pliku CSV"""
    try:
        # Spróbuj najpierw odczytać z nagłówkiem
        try:
            df = pd.read_csv(filepath, parse_dates=['time'], index_col='time')
        except:
            # Jeśli nie uda się z nagłówkiem, wczytaj bez nagłówka
            df = pd.read_csv(filepath, names=['time', 'close'], parse_dates=['time'], index_col='time')
        
        # Jeśli mamy tylko jedną kolumnę z danymi, nadaj jej nazwę 'close'
        if len(df.columns) == 0:
            df = df.rename(columns={df.columns[0]: 'close'})
            
        # Zwróć ostatnie N wierszy
        return df.tail(num_quotes)
    except Exception as e:
        print(f"❌ Błąd odczytu {filepath}: {e}")
        return None

def save_to_csv(data, filename):
    """Zapisuje dane do pliku CSV w katalogu data/reports/"""
    try:
        # Utwórz katalog reports jeśli nie istnieje
        reports_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'reports'))
        os.makedirs(reports_dir, exist_ok=True)
        
        filepath = os.path.join(reports_dir, filename)
        data.to_csv(filepath, index=True, encoding='utf-8')
        return filepath
    except Exception as e:
        print(f"❌ Błąd podczas zapisywania do pliku: {e}")
        return None

def main():
    print(f"🔍 Sprawdzam ostatnie notowania {TIMEFRAME}...\n")
    results = []  # Lista do przechowywania wyników
    
    # Sprawdź czy mamy pełne dane OHLCV
    sample_quotes = None
    for symbol in sorted(os.listdir(BASE_DIR)):
        symbol_path = os.path.join(BASE_DIR, symbol)
        if not os.path.isdir(symbol_path):
            continue
            
        timeframe_path = os.path.join(symbol_path, TIMEFRAME)
        if not os.path.exists(timeframe_path):
            continue
            
        csv_file = f"{symbol}_{TIMEFRAME}.csv"
        csv_path = os.path.join(timeframe_path, csv_file)
        
        if os.path.exists(csv_path):
            sample_quotes = get_last_quotes(csv_path, num_quotes=1)
            if sample_quotes is not None and not sample_quotes.empty:
                break
    
    # Ustal format wyjścia na podstawie dostępnych danych
    has_ohlc = sample_quotes is not None and all(col in sample_quotes.columns for col in ['open', 'high', 'low', 'close'])
    
    if has_ohlc:
        # Pełny format OHLCV
        print(f"{'Symbol':<10} | {'Data':<19} | {'Otwarcie':<10} | {'Najwyższy':<10} | {'Najniższy':<10} | {'Zamknięcie':<10} | {'Wolumen':<8}")
        print("-" * 100)
    else:
        # Uproszczony format tylko z ceną zamknięcia
        print(f"{'Symbol':<10} | {'Data':<19} | {'Wartość':<10}")
        print("-" * 40)
    
    for symbol in sorted(os.listdir(BASE_DIR)):
        symbol_path = os.path.join(BASE_DIR, symbol)
        if not os.path.isdir(symbol_path):
            continue
            
        # Sprawdź czy istnieje katalog z danym timeframe'em
        timeframe_path = os.path.join(symbol_path, TIMEFRAME)
        if not os.path.exists(timeframe_path):
            continue
            
        # Znajdź plik CSV dla danego symbolu i timeframe'u
        csv_file = f"{symbol}_{TIMEFRAME}.csv"
        csv_path = os.path.join(timeframe_path, csv_file)
        
        if os.path.exists(csv_path):
            quotes = get_last_quotes(csv_path, num_quotes=1)  # Pobierz tylko ostatnie notowanie
            
            if quotes is not None and not quotes.empty:
                last_quote = quotes.iloc[-1]
                # Dodaj dane do wyników
                quote_data = {
                    'symbol': symbol,
                    'time': quotes.index[-1],
                    'close': last_quote.iloc[0] if len(last_quote) == 1 else last_quote['close']
                }
                
                # Dodaj dodatkowe kolumny jeśli istnieją
                for col in ['open', 'high', 'low', 'tick_volume']:
                    if col in last_quote.index:
                        quote_data[col] = last_quote[col] if col != 'tick_volume' else int(last_quote[col])
                    else:
                        quote_data[col] = quote_data['close'] if col != 'tick_volume' else 0
                results.append(quote_data)
                
                # Wyświetl w konsoli
                if has_ohlc and all(col in last_quote for col in ['open', 'high', 'low', 'close']):
                    print(f"{symbol:<10} | {quotes.index[-1].strftime('%Y.%m.%d %H:%M')} | "
                          f"{last_quote['open']:<10.5f} | {last_quote['high']:<10.5f} | "
                          f"{last_quote['low']:<10.5f} | {last_quote['close']:<10.5f} | "
                          f"{last_quote.get('tick_volume', 0):<8.0f}")
                else:
                    print(f"{symbol:<10} | {quotes.index[-1].strftime('%Y.%m.%d %H:%M')} | "
                          f"{last_quote.iloc[0]:<10.5f}")
    
    # Zapisz wyniki do pliku CSV jeśli są jakieś dane
    if results:
        df = pd.DataFrame(results).set_index(['symbol', 'time']).sort_index()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'm5_last_quotes_{timestamp}.csv'
        saved_file = save_to_csv(df, filename)
        if saved_file:
            print(f"\n💾 Wyniki zapisano do pliku: {saved_file}")
    
    print(f"\n✅ Zakończono sprawdzanie notowań {TIMEFRAME}")

if __name__ == "__main__":
    main()
