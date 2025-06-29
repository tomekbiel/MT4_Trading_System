import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import os
import json
from datetime import datetime

# Konfiguracja
BASE_DIR = Path(r"C:\\python\\MT4_Trading_System\\data\\historical")
TIME_FRAMES = {
    1: 'M1',
    2: 'M5',
    3: 'M15',
    4: 'H1',
    5: 'H4',
    6: 'D1'
}

def load_symbols():
    """Wczytuje dostępne symbole z katalogów"""
    symbols = [d.name for d in BASE_DIR.iterdir() if d.is_dir() and d.name.endswith('+')]
    return {i+1: sym for i, sym in enumerate(sorted(symbols))}

def load_data(symbol, timeframe):
    """Wczytuje dane dla danego symbolu i timeframe'u"""
    file_path = BASE_DIR / symbol / timeframe / f"{symbol}_{timeframe}.csv"
    if not file_path.exists():
        return None
    
    try:
        df = pd.read_csv(file_path, parse_dates=['time'], index_col='time')
        return df.sort_index()
    except Exception as e:
        print(f"Błąd wczytywania {file_path}: {e}")
        return None

def plot_data(symbol, timeframe, df):
    """Rysuje wykres danych"""
    if df is None or df.empty:
        print(f"Brak danych dla {symbol} {timeframe}")
        return
    
    plt.figure(figsize=(15, 7))
    plt.plot(df.index, df['close'], label='Zamknięcie', linewidth=1)
    
    # Dodaj średnią kroczącą 20 okresów
    if len(df) >= 20:
        df['SMA20'] = df['close'].rolling(window=20).mean()
        plt.plot(df.index, df['SMA20'], label='SMA20', linestyle='--', alpha=0.7)
    
    plt.title(f"{symbol} - {timeframe}\n{df.index[0].date()} do {df.index[-1].date()}")
    plt.xlabel('Data')
    plt.ylabel('Cena')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def show_menu(symbols):
    """Wyświetla menu wyboru"""
    print("\n" + "="*50)
    print("WIZUALIZACJA DANYCH HISTORYCZNYCH")
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
                return None, None
                
            symbol_num = int(choice)
            if symbol_num in symbols:
                symbol = symbols[symbol_num]
                break
            else:
                print("Nieprawidłowy wybór. Spróbuj ponownie.")
        except ValueError:
            print("Wprowadź poprawny numer.")
    
    # Wybór timeframe'u
    print("\nDostępne timeframe'y:")
    for num, tf in TIME_FRAMES.items():
        print(f"{num}. {tf}")
    
    while True:
        try:
            tf_choice = input("\nWybierz numer timeframe'u (0 aby wrócić): ").strip()
            if tf_choice == '0':
                return show_menu(symbols)  # Rekurencyjne wywołanie aby wrócić do menu głównego
                
            tf_num = int(tf_choice)
            if tf_num in TIME_FRAMES:
                timeframe = TIME_FRAMES[tf_num]
                break
            else:
                print("Nieprawidłowy wybór. Spróbuj ponownie.")
        except ValueError:
            print("Wprowadź poprawny numer.")
    
    return symbol, timeframe

def quick_plot_all():
    """Funkcja do szybkiego przeglądania wszystkich symboli i timeframe'ów"""
    symbols = load_symbols()
    if not symbols:
        print("Nie znaleziono żadnych symboli w katalogu danych.")
        return
    
    for symbol in symbols.values():
        for timeframe in TIME_FRAMES.values():
            print(f"\n{'='*50}")
            print(f"Sprawdzam: {symbol} {timeframe}")
            print(f"{'='*50}")
            
            df = load_data(symbol, timeframe)
            if df is not None and not df.empty:
                print(f"Znaleziono {len(df)} świec od {df.index[0]} do {df.index[-1]}")
                plot_data(symbol, timeframe, df)
                
                # Czekaj na naciśnięcie Enter przed przejściem dalej
                user_input = input("Naciśnij Enter aby kontynuować, 's' aby pominąć resztę symbolu, 'q' aby zakończyć: ")
                if user_input.lower() == 'q':
                    return
                elif user_input.lower() == 's':
                    break  # Przerwij pętlę po timeframe'ach dla tego symbolu
            else:
                print(f"Brak danych dla {symbol} {timeframe}")
                input("Naciśnij Enter, aby kontynuować...")

def main():
    # ============================================
    # SEKCJA DO KOMENTOWANIA - WYBIERZ JEDNĄ OPCJĘ:
    # ============================================
    
    # OPCJA 1: Automatyczne przeglądanie wszystkich symboli i timeframe'ów
    # (odkomentuj poniższą linię, aby włączyć)
    quick_plot_all()
    return
    symbols = load_symbols()
    if not symbols:
        print("Nie znaleziono żadnych symboli w katalogu danych.")
        return
    
    print(f"Znaleziono {len(symbols)} symboli.")
    
    # Główna pętla programu
    while True:
        symbol, timeframe = show_menu(symbols)
        
        if symbol is None or timeframe is None:
            print("Zakończono program.")
            break
        
        # Wczytaj i wyświetl dane
        print(f"\nWczytywanie danych dla {symbol} {timeframe}...")
        df = load_data(symbol, timeframe)
        
        if df is not None and not df.empty:
            print(f"Znaleziono {len(df)} świec od {df.index[0]} do {df.index[-1]}")
            plot_data(symbol, timeframe, df)
        else:
            print(f"Nie znaleziono danych dla {symbol} {timeframe}")
        
        input("\nNaciśnij Enter, aby kontynuować...")

if __name__ == "__main__":
    main()
