import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import os
import json
from datetime import datetime

# Konfiguracja
BASE_DIR = Path(r"C:\\python\\MT4_Trading_System\\data\\historical")
CONFIG_DIR = Path(r"C:\\python\\MT4_Trading_System\\config")

def load_timeframes():
    """Wczytuje dostępne timeframe'y z pliku JSON"""
    try:
        with open(CONFIG_DIR / 'timeframes.json', 'r') as f:
            config = json.load(f)
            timeframes = config.get('timeframes', [])
            return {i+1: tf for i, tf in enumerate(timeframes)}
    except Exception as e:
        print(f"Błąd wczytywania pliku konfiguracyjnego: {e}")
        # Domyślne timeframe'y w przypadku błędu
        return {1: 'M1', 2: 'M5', 3: 'M15', 4: 'H1', 5: 'H4', 6: 'D1'}

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

def show_timeframe_menu(timeframes):
    """Wyświetla menu wyboru timeframe'u"""
    print("\n" + "="*50)
    print("WYBÓR TIMEFRAME'U")
    print("="*50)
    
    # Wybór timeframe'u
    print("\nDostępne timeframe'y:")
    for num, tf in timeframes.items():
        print(f"{num}. {tf}")
    
    while True:
        try:
            tf_choice = input("\nWybierz numer timeframe'u (0 aby wyjść): ").strip()
            if tf_choice == '0':
                return None
                
            tf_num = int(tf_choice)
            if tf_num in timeframes:
                return timeframes[tf_num]
            else:
                print("Nieprawidłowy wybór. Spróbuj ponownie.")
        except ValueError:
            print("Wprowadź poprawny numer.")

def show_symbols_for_timeframe(timeframe):
    """Wyświetla wszystkie dostępne symbole dla danego timeframe'u"""
    symbols = load_symbols()
    if not symbols:
        print("Nie znaleziono żadnych symboli w katalogu danych.")
        return
    
    print(f"\n{'='*50}")
    print(f"PRZEGLĄDANIE WSZYSTKICH SYMBOLI DLA TIMEFRAME'U: {timeframe}")
    print(f"{'='*50}")
    
    for num, symbol in symbols.items():
        print(f"\nSprawdzam: {symbol} {timeframe}")
        df = load_data(symbol, timeframe)
        
        if df is not None and not df.empty:
            print(f"Znaleziono {len(df)} świec od {df.index[0]} do {df.index[-1]}")
            plot_data(symbol, timeframe, df)
            
            # Czekaj na naciśnięcie Enter przed przejściem dalej
            user_input = input("Naciśnij Enter aby kontynuować, 'q' aby zakończyć: ")
            if user_input.lower() == 'q':
                break
        else:
            print(f"Brak danych dla {symbol} {timeframe}")
            input("Naciśnij Enter, aby kontynuować...")

def quick_plot_all(timeframes):
    """Funkcja do szybkiego przeglądania wszystkich symboli i timeframe'ów"""
    symbols = load_symbols()
    if not symbols:
        print("Nie znaleziono żadnych symboli w katalogu danych.")
        return
    
    for symbol in symbols.values():
        for timeframe in timeframes.values():
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
    # Wczytaj dostępne timeframe'y z pliku JSON
    timeframes = load_timeframes()
    
    # Główna pętla programu
    while True:
        # Najpierw wybierz timeframe
        timeframe = show_timeframe_menu(timeframes)
        
        if timeframe is None:
            print("Zakończono program.")
            break
        
        # Następnie pokaż wszystkie symbole dla wybranego timeframe'u
        show_symbols_for_timeframe(timeframe)

if __name__ == "__main__":
    main()
