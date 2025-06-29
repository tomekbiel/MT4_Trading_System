import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime, timedelta
import sys
import matplotlib.pyplot as plt

# Konfiguracja
BASE_DIR = Path(r"C:\python\MT4_Trading_System\data\historical")
LAST_HOURS_TO_KEEP = 72  # Ostatnie godziny danych, które uznajemy za wiarygodne
MAX_PRICE_CHANGE_PCT = 50  # Maksymalna akceptowalna zmiana ceny w %

def get_available_timeframes():
    """Pobierz dostępne ramy czasowe z katalogu danych"""
    if not BASE_DIR.exists():
        return []
    
    # Pobierz pierwszy dostępny symbol
    symbols = [d for d in os.listdir(BASE_DIR) 
              if (BASE_DIR / d).is_dir() and not d.startswith('.')]
    
    if not symbols:
        return []
    
    # Pobierz ramy czasowe z pierwszego symbola
    timeframes_dir = BASE_DIR / symbols[0]
    timeframes = [d for d in os.listdir(timeframes_dir) 
                 if (timeframes_dir / d).is_dir() and not d.startswith('.')]
    
    return sorted(timeframes, key=lambda x: (len(x), x))

def select_timeframe():
    """Pozwól użytkownikowi wybrać ramę czasową"""
    timeframes = get_available_timeframes()
    
    if not timeframes:
        print("Nie znaleziono dostępnych ram czasowych.")
        return None
    
    print("\nDostępne ramy czasowe:")
    for i, tf in enumerate(timeframes, 1):
        print(f"{i}. {tf}")
    
    while True:
        try:
            choice = input("\nWybierz numer ramy czasowej: ")
            if not choice.strip():
                return timeframes[0]  # Domyślnie pierwsza rama czasowa
                
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(timeframes):
                return timeframes[choice_idx]
            print("Nieprawidłowy wybór. Spróbuj ponownie.")
        except ValueError:
            print("Proszę wprowadzić liczbę.")

def clear_screen():
    """Czyści ekran konsoli"""
    os.system('cls' if os.name == 'nt' else 'clear')

def load_data(filepath):
    """Wczytuje dane z pliku CSV, obsługując różne formaty"""
    try:
        # Spróbuj wczytać z nagłówkiem
        try:
            df = pd.read_csv(filepath, parse_dates=['time'], index_col='time')
            if 'close' not in df.columns and len(df.columns) > 0:
                df = df.rename(columns={df.columns[0]: 'close'})
        except:
            # Jeśli nie uda się z nagłówkiem, wczytaj bez nagłówka
            df = pd.read_csv(filepath, names=['time', 'close'], parse_dates=['time'], index_col='time')
        
        if df.empty:
            return None, "Plik jest pusty"
            
        # Sprawdź, czy mamy kolumnę close (lub pierwszą kolumnę z danymi)
        if 'close' not in df.columns and len(df.columns) > 0:
            df = df.rename(columns={df.columns[0]: 'close'})
            
        return df.sort_index(), None
    except Exception as e:
        return None, f"Błąd wczytywania: {e}"

def analyze_file(symbol, timeframe):
    """Analizuje pojedynczy plik CSV"""
    file_path = BASE_DIR / symbol / timeframe / f"{symbol}_{timeframe}.csv"
    if not file_path.exists():
        return None, None, None, f"Nie znaleziono pliku: {file_path}"
    
    # Wczytaj dane
    df, error = load_data(file_path)
    if error:
        return None, None, None, error
    
    if df.empty:
        return None, None, None, "Brak danych w pliku"
    
    # Znajdź ostatnią datę w danych
    last_date = df.index.max()
    threshold_date = last_date - timedelta(hours=LAST_HOURS_TO_KEEP)
    
    # Podziel dane na wiarygodne (ostatnie godziny) i do weryfikacji
    last_valid = df[df.index >= threshold_date]
    to_check = df[df.index < threshold_date]
    
    if last_valid.empty:
        return None, None, None, "Brak wystarczających danych do weryfikacji"
    
    # Oblicz średnią z ostatnich wiarygodnych danych
    avg_recent = last_valid['close'].mean()
    
    # Znajdź wartości odstające
    to_check['pct_diff'] = ((to_check['close'] - avg_recent) / avg_recent * 100).abs()
    anomalies = to_check[to_check['pct_diff'] > MAX_PRICE_CHANGE_PCT].copy()
    
    return df, last_valid, anomalies, None

def clean_and_save(df, anomalies, symbol, timeframe):
    """Usuwa anomalię i zapisuje dane, zachowując oryginalny format znacznika czasu"""
    if df is None or anomalies is None or anomalies.empty:
        return False, "Brak danych do zapisania"
    
    try:
        # Utwórz kopię danych bez anomali
        clean_df = df[~df.index.isin(anomalies.index)].copy()
        
        # Ścieżka do pliku docelowego
        file_path = BASE_DIR / symbol / timeframe / f"{symbol}_{timeframe}.csv"
        
        # Sprawdź, czy plik źródłowy ma nagłówek
        has_header = False
        try:
            with open(file_path, 'r') as f:
                first_line = f.readline().strip()
                if first_line.startswith('time,') or first_line.startswith('date,'):
                    has_header = True
        except Exception:
            pass
        
        # Zapisz dane z odpowiednim formatem daty i nagłówkiem
        if has_header:
            # Zapis z nagłówkiem
            clean_df.to_csv(file_path, 
                          date_format='%Y.%m.%d %H:%M', 
                          index=True, 
                          header=True,
                          index_label='time')
        else:
            # Zapis bez nagłówka
            clean_df.to_csv(file_path, 
                          date_format='%Y.%m.%d %H:%M', 
                          index=True, 
                          header=False)
        
        return True, f"Usunięto {len(anomalies)} wierszy i zapisano plik"
    except Exception as e:
        return False, f"Błąd podczas zapisywania: {e}"

def print_analysis(symbol, df, last_valid, anomalies, error=None, timeframe=None):
    """Wyświetla wyniki analizy dla pojedynczego pliku"""
    clear_screen()
    print("\n" + "="*80)
    print(f"ANALIZA: {symbol} - {timeframe if timeframe else 'N/A'}".center(80))
    print("="*80)
    
    if error:
        print(f"\n❌ BŁĄD: {error}")
        return
    
    # Podstawowe informacje o pliku
    print(f"\n{'Ostatnia data w pliku:':<25} {df.index.max()}")
    print(f"{'Liczba wszystkich wpisów:':<25} {len(df):,}".replace(',', ' '))
    
    # Informacje o ostatnich wiarygodnych danych
    if last_valid is not None and not last_valid.empty:
        print(f"\n{'Ostatnie wiarygodne dane:':<25} {len(last_valid):,} wpisów (ostatnie {LAST_HOURS_TO_KEEP}h)".replace(',', ' '))
        print(f"{'Średnia wartość:':<25} {last_valid['close'].mean():.5f}")
        print(f"{'Zakres wartości:':<25} {last_valid['close'].min():.5f} - {last_valid['close'].max():.5f}")
    
    # Informacje o znalezionych anomaliach
    if anomalies is not None and not anomalies.empty:
        print(f"\n🔍 ZNALEZIONO ANOMALIE ({len(anomalies)}):")
        print("-" * 80)
        print(f"{'Data':<25} {'Wartość':<15} {'Różnica %':<15} 'Różnica bezwzględna'")
        print("-" * 80)
        
        avg_recent = last_valid['close'].mean()
        for date, row in anomalies.iterrows():
            abs_diff = abs(row['close'] - avg_recent)
            print(f"{date}  {row['close']:>15.5f}  {row['pct_diff']:>14.2f}%  {abs_diff:>15.5f}")
    else:
        print("\n✅ Nie znaleziono żadnych anomalii w danych.")

def process_symbol(symbol, timeframe):
    """Przetwarza pojedynczy symbol dla wybranej ramy czasowej"""
    df, last_valid, anomalies, error = analyze_file(symbol, timeframe)
    
    while True:
        print_analysis(symbol, df, last_valid, anomalies, error, timeframe)
        
        if error or anomalies is None or anomalies.empty:
            break
            
        print("\n" + "-"*80)
        print("Co chcesz zrobić?")
        print("  d - Usuń znalezione anomalię i zapisz zmiany")
        print("  n - Przejdź do następnego symbolu")
        print("  q - Zakończ program")
        
        choice = input("\nTwój wybór (d/n/q): ").strip().lower()
        
        if choice == 'd':
            success, message = clean_and_save(df, anomalies, symbol, timeframe)
            if success:
                print(f"\n✅ {message}")
                input("\nNaciśnij Enter, aby kontynuować...")
                # Po usunięciu, przeładuj dane i sprawdź ponownie
                df, last_valid, anomalies, error = analyze_file(symbol, timeframe)
                if not anomalies.empty:
                    print("\n⚠️  Nadal istnieją anomalię. Sprawdź ponownie.")
                    input("Naciśnij Enter, aby kontynuować...")
                else:
                    print("\n✅ Wszystkie anomalię zostały usunięte.")
                    input("Naciśnij Enter, aby przejść do następnego symbolu...")
                    break
            else:
                print(f"\n❌ {message}")
                input("Naciśnij Enter, aby spróbować ponownie...")
        elif choice == 'n':
            break
        elif choice == 'q':
            return True  # Zakończ program
    
    return False  # Kontynuuj z następnym symbolem

def load_timeframes():
    """Wczytuje dostępne ramy czasowe z pliku konfiguracyjnego"""
    try:
        config_path = BASE_DIR.parent.parent / 'config' / 'timeframes.json'
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get('timeframes', ['M1', 'M5', 'M15', 'H1', 'H4', 'D1'])
    except Exception as e:
        print(f"Błąd wczytywania konfiguracji ram czasowych, używam domyślnych: {e}")
        return ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']

def main():
    # Wczytaj dostępne ramy czasowe
    timeframes = load_timeframes()
    print("Dostępne ramy czasowe:", ", ".join(timeframes))
    
    # Wybierz ramę czasową
    while True:
        timeframe = input("\nWybierz ramę czasową (np. M5, H1, D1): ").strip().upper()
        if timeframe in timeframes:
            break
        print(f"Nieprawidłowa rama czasowa. Wybierz spośród: {', '.join(timeframes)}")
    
    # Znajdź wszystkie dostępne symbole
    symbols = [d.name for d in BASE_DIR.iterdir() if d.is_dir() and d.name.endswith('+')]
    symbols.sort()
    
    if not symbols:
        print("Nie znaleziono żadnych symboli do analizy.")
        return
    
    print(f"\nZnaleziono {len(symbols)} symboli do analizy.")
    
    # Przetwarzaj każdy symbol po kolei dla wybranej ramy czasowej
    for symbol in symbols:
        if process_symbol(symbol, timeframe):
            print("\nZakończono działanie programu.")
            break
        
        # Pytanie o kontynuację po każdym symbolu
        if symbol != symbols[-1]:
            print("\n" + "-"*80)
            cont = input("Czy chcesz przejść do następnego symbolu? (t/n): ").strip().lower()
            if cont != 't':
                print("\nZakończono działanie programu.")
                break
    else:
        print("\nZakończono analizę wszystkich symboli.")

if __name__ == "__main__":
    import json  # Dodaj import na początku bloku
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nPrzerwano działanie programu przez użytkownika.")
    except Exception as e:
        print(f"\n\nWystąpił nieoczekiwany błąd: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nNaciśnij Enter, aby zakończyć...")
        input()
