import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import os
from datetime import datetime

# Konfiguracja
BASE_DIR = Path(r"C:\python\MT4_Trading_System\data\historical")
TIMEFRAME = 'M1'  # Tylko interwał M1

def load_symbols():
    """Wczytuje dostępne symbole z katalogów"""
    symbols = [d.name for d in BASE_DIR.iterdir() if d.is_dir() and d.name.endswith('+')]
    return {i+1: sym for i, sym in enumerate(sorted(symbols))}

def load_data(symbol):
    """Wczytuje dane M1 dla danego symbolu"""
    file_path = BASE_DIR / symbol / TIMEFRAME / f"{symbol}_{TIMEFRAME}.csv"
    if not file_path.exists():
        return None
    
    try:
        df = pd.read_csv(file_path, parse_dates=['time'], index_col='time')
        return df.sort_index()
    except Exception as e:
        print(f"Błąd wczytywania {file_path}: {e}")
        return None

def plot_m1_data(symbol, df):
    """Rysuje wykres danych M1 z dodatkowymi wskaźnikami"""
    if df is None or df.empty:
        print(f"Brak danych dla {symbol} {TIMEFRAME}")
        return
    
    plt.figure(figsize=(15, 10))
    
    # Wykres świecowy
    plt.subplot(2, 1, 1)
    plt.plot(df.index, df['close'], label='Zamknięcie', linewidth=0.8, color='blue')
    
    # Dodaj średnie kroczące
    df['SMA20'] = df['close'].rolling(window=20).mean()
    df['SMA50'] = df['close'].rolling(window=50).mean()
    
    plt.plot(df.index, df['SMA20'], label='SMA20', linestyle='--', alpha=0.8, color='orange')
    plt.plot(df.index, df['SMA50'], label='SMA50', linestyle='--', alpha=0.8, color='red')
    
    plt.title(f"{symbol} - {TIMEFRAME}\n{df.index[0].date()} do {df.index[-1].date()}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Wykres wolumenu
    plt.subplot(2, 1, 2)
    plt.bar(df.index, df['tick_volume'], color='gray', alpha=0.7, width=0.0005)
    plt.title('Wolumen transakcji')
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.show()

def show_menu(symbols):
    """Wyświetla menu wyboru symbolu"""
    print("\n" + "="*50)
    print("WIZUALIZACJA DANYCH M1")
    print("="*50)
    
    # Wyświetl dostępne symbole
    print("\nDostępne symbole:")
    for num, sym in symbols.items():
        print(f"{num:2d}. {sym}")
    
    # Wybór symbolu
    while True:
        try:
            choice = input("\nWybierz numer symbolu (0 aby wyjść): ").strip()
            if choice == '0':
                return None
                
            symbol_num = int(choice)
            if symbol_num in symbols:
                return symbols[symbol_num]
            else:
                print("Nieprawidlowy wybor. Sprobuj ponownie.")
        except ValueError:
            print("Wprowadz poprawny numer.")

def quick_view_all_symbols():
    """Szybki podgląd wszystkich dostępnych symboli"""
    symbols = load_symbols()
    if not symbols:
        print("Nie znaleziono żadnych symboli w katalogu danych.")
        return
    
    for symbol in symbols.values():
        print(f"\n{'='*50}")
        print(f"Sprawdzam: {symbol} {TIMEFRAME}")
        print(f"{'='*50}")
        
        df = load_data(symbol)
        if df is not None and not df.empty:
            print(f"Znaleziono {len(df)} świec od {df.index[0]} do {df.index[-1]}")
            plot_m1_data(symbol, df)
            
            # Czekaj na naciśnięcie Enter przed przejściem dalej
            user_input = input("Naciśnij Enter aby kontynuować, 'q' aby zakończyć: ")
            if user_input.lower() == 'q':
                return
        else:
            print(f"Brak danych dla {symbol} {TIMEFRAME}")
            input("Naciśnij Enter, aby kontynuować...")

def main():
    print("""
    WIZUALIZACJA DANYCH M1
    ----------------------
    1. Wybierz symbol do wyświetlenia
    2. Szybki podgląd wszystkich symboli
    0. Wyjście
    """)
    
    while True:
        choice = input("Wybierz opcję (0-2): ").strip()
        
        if choice == '0':
            print("Zakończono program.")
            break
        elif choice == '1':
            symbols = load_symbols()
            if not symbols:
                print("Nie znaleziono żadnych symboli w katalogu danych.")
                continue
                
            symbol = show_menu(symbols)
            if symbol:
                df = load_data(symbol)
                if df is not None and not df.empty:
                    print(f"Znaleziono {len(df)} świec od {df.index[0]} do {df.index[-1]}")
                    plot_m1_data(symbol, df)
                else:
                    print(f"Nie znaleziono danych dla {symbol} {TIMEFRAME}")
                input("\nNaciśnij Enter, aby kontynuować...")
                
        elif choice == '2':
            quick_view_all_symbols()
        else:
            print("Nieprawidlowy wybor. Sprobuj ponownie.")

if __name__ == "__main__":
    main()
