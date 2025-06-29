import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import os
import time

# Konfiguracja
BASE_DIR = Path(r"C:\python\MT4_Trading_System\data\historical")
TIMEFRAME = 'M5'
DELAY_BETWEEN_PLOTS = 2  # sekundy pomiędzy wykresami

def load_symbols():
    """Wczytuje dostępne symbole z katalogów"""
    symbols = [d.name for d in BASE_DIR.iterdir() if d.is_dir() and d.name.endswith('+')]
    return sorted(symbols)

def load_data(symbol):
    """Wczytuje dane M5 dla danego symbolu"""
    file_path = BASE_DIR / symbol / TIMEFRAME / f"{symbol}_{TIMEFRAME}.csv"
    if not file_path.exists():
        return None, f"Brak pliku: {file_path}"
    
    try:
        # Spróbuj wczytać z nagłówkiem
        try:
            df = pd.read_csv(file_path, parse_dates=['time'], index_col='time')
            # Jeśli mamy kolumnę 'close', użyj jej, w przeciwnym razie użyj pierwszej kolumny
            if 'close' not in df.columns and len(df.columns) > 0:
                df = df.rename(columns={df.columns[0]: 'close'})
        except:
            # Jeśli nie uda się z nagłówkiem, wczytaj bez nagłówka
            df = pd.read_csv(file_path, names=['time', 'close'], parse_dates=['time'], index_col='time')
        
        if df.empty:
            return None, "Plik jest pusty"
            
        return df.sort_index(), None
    except Exception as e:
        return None, f"Błąd wczytywania: {e}"

def plot_simple(symbol, df):
    """Rysuje uproszczony wykres ceny zamknięcia"""
    plt.figure(figsize=(12, 4))
    
    # Pobierz dane do wykresu
    if 'close' in df.columns:
        close_prices = df['close']
    else:
        close_prices = df[df.columns[0]]
    
    # Narysuj linię ceny zamknięcia
    plt.plot(close_prices.index, close_prices, linewidth=1, color='blue')
    
    # Ustawienia wykresu
    plt.title(f"{symbol} - {TIMEFRAME}\n{df.index[0].date()} - {df.index[-1].date()}  ({len(df)} świec)")
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    
    # Dodaj min/max wartości
    min_val = close_prices.min()
    max_val = close_prices.max()
    plt.figtext(0.1, 0.01, f"Min: {min_val:.5f}  |  Max: {max_val:.5f}  |  Różnica: {max_val-min_val:.5f} ({((max_val/min_val)-1)*100:.2f}%)", 
                ha="left", fontsize=9)
    
    plt.tight_layout()
    plt.show()

def main():
    # Wczytaj dostępne symbole
    symbols = load_symbols()
    if not symbols:
        print("Nie znaleziono żadnych symboli w katalogu danych.")
        return
    
    print(f"\nZnaleziono {len(symbols)} symboli. Rozpoczynam wyświetlanie...")
    print("Naciśnij Ctrl+C, aby zakończyć wcześniej.\n")
    
    for symbol in symbols:
        try:
            print(f"\n{'='*60}")
            print(f"Przetwarzanie: {symbol}...")
            
            df, error = load_data(symbol)
            if df is None:
                print(f"  ❌ {error}")
                continue
                
            print(f"  ✓ Znaleziono {len(df)} świec")
            print(f"  ✓ Zakres dat: {df.index[0]} - {df.index[-1]}")
            
            if 'close' in df.columns:
                close_prices = df['close']
            else:
                close_prices = df[df.columns[0]]
                
            min_val = close_prices.min()
            max_val = close_prices.max()
            print(f"  ✓ Wartości: {min_val:.5f} - {max_val:.5f} (różnica: {max_val-min_val:.5f}, {((max_val/min_val)-1)*100:.2f}%)")
            
            # Wyświetl wykres
            plot_simple(symbol, df)
            
            # Oczekiwanie przed następnym wykresem
            if symbol != symbols[-1]:  # Nie czekaj po ostatnim symbolu
                print(f"\nNastępny wykres za {DELAY_BETWEEN_PLOTS} sekund... (Ctrl+C aby przerwać)")
                time.sleep(DELAY_BETWEEN_PLOTS)
                
        except KeyboardInterrupt:
            print("\nPrzerwano przez użytkownika.")
            break
        except Exception as e:
            print(f"  ❌ Błąd podczas przetwarzania {symbol}: {e}")
            continue
    
    print("\nZakończono przetwarzanie wszystkich symboli.")

if __name__ == "__main__":
    main()
