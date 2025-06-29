import os
import shutil
from pathlib import Path

# Konfiguracja
BASE_DIR = Path(r"/data/historical")
TIMEFRAME = 'M1'
KEEP_SYMBOLS = {'US.100+', 'oil.wti+'}  # Symbole, które mają zostać zachowane

def remove_m1_except_keep():
    """Usuwa wszystkie pliki M1 z wyjątkiem określonych symboli"""
    removed_count = 0
    
    # Przejdź przez wszystkie katalogi symboli
    for symbol_dir in os.listdir(BASE_DIR):
        symbol_path = BASE_DIR / symbol_dir
        
        # Sprawdź czy to katalog i czy nie jest na liście wyjątków
        if symbol_path.is_dir() and symbol_dir not in KEEP_SYMBOLS:
            m1_dir = symbol_path / TIMEFRAME
            
            # Sprawdź czy istnieje katalog M1
            if m1_dir.exists() and m1_dir.is_dir():
                try:
                    # Usuń cały katalog M1
                    shutil.rmtree(m1_dir)
                    print(f"Usunięto: {m1_dir}")
                    removed_count += 1
                except Exception as e:
                    print(f"Błąd podczas usuwania {m1_dir}: {e}")
    
    print(f"\nZakończono. Usunięto katalogi M1 dla {removed_count} symboli.")
    print(f"Zachowano dane M1 dla: {', '.join(KEEP_SYMBOLS)}")

if __name__ == "__main__":
    print("UWAGA: Ten skrypt usunie wszystkie pliki M1 z wyjątkiem:")
    for symbol in sorted(KEEP_SYMBOLS):
        print(f"  - {symbol}")
    
    confirm = input("\nCzy na pewno chcesz kontynuować? (wpisz 'TAK' aby potwierdzić): ")
    
    if confirm.strip().upper() == 'TAK':
        remove_m1_except_keep()
    else:
        print("Anulowano operację.")
