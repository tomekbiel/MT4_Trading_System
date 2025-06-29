import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
from datetime import datetime
import glob

def get_available_symbols():
    """Pobierz dostępne symbole z katalogu danych"""
    data_dir = os.path.join('..', '..', 'data', 'historical')
    if not os.path.exists(data_dir):
        return []
    return [d for d in os.listdir(data_dir) 
            if os.path.isdir(os.path.join(data_dir, d)) and 
            not d.startswith('.')]

def load_data(symbol, timeframe):
    """Wczytaj dane dla danego symbolu i ramy czasowej"""
    file_path = os.path.join('..', '..', 'data', 'historical', 
                           symbol, timeframe, f"{symbol}_{timeframe}.csv")
    
    if not os.path.exists(file_path):
        return None
    
    try:
        df = pd.read_csv(file_path, parse_dates=['time'], index_col='time')
        if 'close' not in df.columns and len(df.columns) > 0:
            df = df.rename(columns={df.columns[0]: 'close'})
        return df[['close']]  # Zwróć tylko kolumnę close
    except Exception as e:
        print(f"Błąd wczytywania pliku {file_path}: {e}")
        return None

def plot_timeframe(ax, df, symbol, timeframe):
    """Narysuj wykres dla danej ramy czasowej"""
    if df is None or df.empty:
        return False
    
    # Formatowanie daty
    date_format = "%m-%d %H:%M" if len(df) < 100 else "%m-%d"
    
    # Rysowanie wykresu
    ax.plot(df.index, df['close'], label=f'{timeframe} Close')
    
    # Formatowanie osi X
    ax.xaxis.set_major_formatter(mdates.DateFormatter(date_format))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    # Tytuł i legenda
    ax.set_title(f'{symbol} {timeframe} - Close Price')
    ax.legend()
    ax.grid(True)
    
    # Dodanie zakresu dat
    date_range = f"{df.index[0].strftime('%Y-%m-%d')} to {df.index[-1].strftime('%Y-%m-%d')}"
    ax.set_xlabel(f'Date Range: {date_range}\nData Points: {len(df)}')
    
    return True

def main():
    # Pobierz dostępne symbole
    symbols = get_available_symbols()
    if not symbols:
        print("Nie znaleziono żadnych danych. Uruchom najpierw skrypt pobierający dane.")
        return
    
    # Wybór symbolu
    print("\nDostępne instrumenty:")
    for i, symbol in enumerate(symbols, 1):
        print(f"{i}. {symbol}")
    
    while True:
        try:
            choice = int(input("\nWybierz numer instrumentu: ")) - 1
            if 0 <= choice < len(symbols):
                selected_symbol = symbols[choice]
                break
            else:
                print("Nieprawidłowy wybór. Spróbuj ponownie.")
        except ValueError:
            print("Proszę wprowadzić liczbę.")
    
    # Dostępne ramy czasowe
    timeframes = ['M1', 'M5', 'M15', 'H1', 'H4', 'D1']
    
    # Przygotowanie wykresu
    fig, axes = plt.subplots(len(timeframes), 1, figsize=(14, 3*len(timeframes)))
    fig.suptitle(f'Analiza instrumentu {selected_symbol} - Wykresy zamknięć', fontsize=16)
    
    if len(timeframes) == 1:
        axes = [axes]  # Upewnij się, że axes jest listą
    
    # Generowanie wykresów dla każdej ramy czasowej
    for i, tf in enumerate(timeframes):
        df = load_data(selected_symbol, tf)
        if df is not None and not df.empty:
            plot_timeframe(axes[i], df, selected_symbol, tf)
    
    # Dopasowanie układu i wyświetlenie
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.subplots_adjust(hspace=0.5)
    plt.show()

if __name__ == "__main__":
    print("="*80)
    print("ANALIZA INSTRUMENTU - WYKRESY ZAMKNIĘĆ")
    print("="*80)
    print("Ten skrypt wyświetla wykresy cen zamknięcia dla wybranego instrumentu")
    print("we wszystkich dostępnych ramach czasowych (M1, M5, M15, H1, H4, D1).")
    print("-"*80)
    
    main()
