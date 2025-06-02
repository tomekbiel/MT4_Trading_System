import pandas as pd
import os

# Ścieżka do folderu z danymi
BASE_DIR = r"C:\python\MT4_Trading_System\data\historical\US.100+\M1"

# Nazwy plików do połączenia (zmień na swoje)
FILE1 = "US.100+_M1.csv"
FILE2 = "US.100+_M2.csv"
OUTPUT_FILE = "US.100+_M1.csv"


def merge_csv_by_time():
    """
    Łączy dwa pliki CSV po kolumnie czasu, zachowując wszystkie dane.
    - Wczytuje oba pliki
    - Łączy je zachowując wszystkie wiersze
    - Sortuje po dacie
    - Zapisuje do nowego pliku
    """
    try:
        # Wczytaj oba pliki
        df1 = pd.read_csv(os.path.join(BASE_DIR, FILE1))
        df2 = pd.read_csv(os.path.join(BASE_DIR, FILE2))

        # Sprawdź czy oba mają kolumnę czasu
        if 'time' not in df1.columns or 'time' not in df2.columns:
            print("❌ Oba pliki muszą mieć kolumnę 'time'")
            return

        # Połącz pliki
        merged = pd.concat([df1, df2], ignore_index=True)

        # Konwertuj czas na datetime dla poprawnego sortowania
        merged['time'] = pd.to_datetime(merged['time'])

        # Posortuj po czasie
        merged.sort_values('time', inplace=True)

        # Usuń duplikaty (zachowuje ostatnie wystąpienie)
        merged.drop_duplicates(subset='time', keep='last', inplace=True)

        # Zapisz wynik
        output_path = os.path.join(BASE_DIR, OUTPUT_FILE)
        merged.to_csv(output_path, index=False)

        print(f"✅ Połączono pliki. Wynik zapisano w: {output_path}")
        print(f"Liczba wierszy: {len(merged)}")

    except Exception as e:
        print(f"❌ Błąd podczas łączenia plików: {str(e)}")


if __name__ == "__main__":
    print("Łączenie plików CSV...")
    merge_csv_by_time()