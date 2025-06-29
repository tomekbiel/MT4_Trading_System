import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime, timedelta
import sys

# Konfiguracja
BASE_DIR = Path(r"C:\python\MT4_Trading_System\data\historical")
TIMEFRAME = 'M5'
LAST_HOURS_TO_KEEP = 72  # Ostatnie godziny danych, które uznajemy za wiarygodne
MAX_PRICE_CHANGE_PCT = 50  # Maksymalna akceptowalna zmiana ceny w %

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

def analyze_file(symbol):
    """Analizuje pojedynczy plik CSV"""
    file_path = BASE_DIR / symbol / TIMEFRAME / f"{symbol}_{TIMEFRAME}.csv"
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

def clean_and_save(df, anomalies, symbol):
    """Usuwa anomalię i zapisuje dane"""
    if df is None or anomalies is None or anomalies.empty:
        return False, "Brak danych do zapisania"
    
    try:
        # Usuń anomalię
        clean_df = df[~df.index.isin(anomalies.index)]
        
        # Zapisz z powrotem do pliku z odpowiednim formatem daty
        file_path = BASE_DIR / symbol / TIMEFRAME / f"{symbol}_{TIMEFRAME}.csv"
        
        # Konwersja daty do odpowiedniego formatu
        clean_df.index = clean_df.index.strftime('%Y.%m.%d %H:%M')
        
        # Zapis do pliku bez dodatkowych cudzysłowów i z odpowiednim formatem
        clean_df.to_csv(file_path, index=True, header=True, quoting=0, quotechar='\0')
        
        # Ponowne wczytanie i zapisanie, aby upewnić się, że format jest poprawny
        clean_df = pd.read_csv(file_path, parse_dates=[0], index_col=0)
        clean_df.index = pd.to_datetime(clean_df.index).strftime('%Y.%m.%d %H:%M')
        clean_df.to_csv(file_path, date_format='%Y.%m.%d %H:%M', index=True, header=True, quoting=0, quotechar='\0')
        
        return True, f"Usunięto {len(anomalies)} wierszy i zapisano plik"
    except Exception as e:
        return False, f"Błąd podczas zapisywania: {e}"

def print_analysis(symbol, df, last_valid, anomalies, error=None):
    """Wyświetla wyniki analizy dla pojedynczego pliku"""
    clear_screen()
    print("\n" + "="*80)
    print(f"ANALIZA: {symbol} - {TIMEFRAME}".center(80))
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

def process_symbol(symbol):
    """Przetwarza pojedynczy symbol"""
    df, last_valid, anomalies, error = analyze_file(symbol)
    
    while True:
        print_analysis(symbol, df, last_valid, anomalies, error)
        
        if error or anomalies is None or anomalies.empty:
            break
            
        print("\n" + "-"*80)
        print("Co chcesz zrobić?")
        print("  d - Usuń znalezione anomalię i zapisz zmiany")
        print("  n - Przejdź do następnego symbolu")
        print("  q - Zakończ program")
        
        choice = input("\nTwój wybór (d/n/q): ").strip().lower()
        
        if choice == 'd':
            success, message = clean_and_save(df, anomalies, symbol)
            if success:
                print(f"\n✅ {message}")
                input("\nNaciśnij Enter, aby kontynuować...")
                # Po usunięciu, przeładuj dane i sprawdź ponownie
                df, last_valid, anomalies, error = analyze_file(symbol)
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

def main():
    # Znajdź wszystkie dostępne symbole
    symbols = [d.name for d in BASE_DIR.iterdir() if d.is_dir() and d.name.endswith('+')]
    symbols.sort()
    
    if not symbols:
        print("Nie znaleziono żadnych symboli do analizy.")
        return
    
    print(f"Znaleziono {len(symbols)} symboli do analizy.")
    
    # Przetwarzaj każdy symbol po kolei
    for symbol in symbols:
        if process_symbol(symbol):
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
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nPrzerwano działanie programu przez użytkownika.")
    except Exception as e:
        print(f"\n\nWystąpił nieoczekiwany błąd: {e}")
    finally:
        print("\nNaciśnij Enter, aby zakończyć...")
        input()
