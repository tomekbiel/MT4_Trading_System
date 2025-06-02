import os
import pandas as pd
from datetime import datetime

# Mapowanie końcówek plików manualnych na docelowe
file_mapping = {
    '1.csv': '_M1.csv',
    '5.csv': '_M5.csv',
    '15.csv': '_M15.csv',
    '60.csv': '_H1.csv',
    '240.csv': '_H4.csv',
    '1440.csv': '_D1.csv'
}

# Ścieżka bazowa do katalogu z danymi
BASE_DIR = r"C:\python\MT4_Trading_System\data\historical"


def convert_to_timestamp(date_str, time_str):
    """Konwertuje różne formaty daty i czasu do wymaganego formatu timestamp"""
    try:
        # Próbuj różne formaty wejściowe
        for fmt in ('%Y.%m.%d %H:%M', '%d/%m/%Y %H:%M', '%d.%m.%Y %H:%M', '%Y-%m-%d %H:%M'):
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                return dt.strftime("%Y.%m.%d %H:%M")
            except ValueError:
                continue
        # Jeśli żaden format nie pasuje, użyj parsowania automatycznego
        dt = pd.to_datetime(f"{date_str} {time_str}", dayfirst=True)
        return dt.strftime("%Y.%m.%d %H:%M")
    except Exception as e:
        raise ValueError(f"Nie można przekształcić '{date_str} {time_str}' na timestamp: {str(e)}")


def merge_and_replace_data():
    """
    Przetwarza pliki manualne, zachowując dokładny format timestamp (2025.05.13 12:10)
    i obsługując różne formaty dat wejściowych.
    """
    for root, dirs, files in os.walk(BASE_DIR):
        for file in files:
            # Sprawdź czy to plik manualny
            for manual_suffix, auto_suffix in file_mapping.items():
                if file.endswith(manual_suffix):
                    manual_path = os.path.join(root, file)
                    auto_path = os.path.join(root, file.replace(manual_suffix, auto_suffix))

                    try:
                        # Wczytaj plik manualny - sprawdź czy pierwszy wiersz to nagłówek
                        with open(manual_path, 'r') as f:
                            first_line = f.readline().strip()

                        # Sprawdź czy pierwszy wiersz zawiera tekst (nagłówek)
                        has_header = any(c.isalpha() for c in first_line)

                        # Wczytaj dane, pomijając nagłówek jeśli istnieje
                        df_manual = pd.read_csv(manual_path, header=None if has_header else None)

                        # Sprawdź czy plik zawiera dane
                        if df_manual.empty:
                            print(f"⚠️ Plik {file} jest pusty - pomijam")
                            continue

                        # Sprawdź czy plik zawiera co najmniej 7 kolumn
                        if df_manual.shape[1] < 7:
                            raise ValueError(f"Nieprawidłowa liczba kolumn ({df_manual.shape[1]}) w pliku {file}")

                        # Połącz dwie pierwsze kolumny w timestamp w wymaganym formacie
                        df_manual['time'] = df_manual.apply(
                            lambda x: convert_to_timestamp(str(x[0]), str(x[1])),
                            axis=1
                        )

                        # Przypisz pozostałe kolumny
                        df_manual['open'] = df_manual[2]
                        df_manual['high'] = df_manual[3]
                        df_manual['low'] = df_manual[4]
                        df_manual['close'] = df_manual[5]
                        df_manual['volume'] = df_manual[6]

                        # Dodaj brakujące kolumny
                        df_manual['spread'] = 0
                        df_manual['real_volume'] = 0

                        # Wybierz tylko potrzebne kolumny
                        final_columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'spread', 'real_volume']
                        df_manual = df_manual[final_columns]

                        # Jeśli plik automatyczny istnieje, wczytaj go
                        if os.path.exists(auto_path):
                            df_auto = pd.read_csv(auto_path)

                            # Znajdź ostatnią datę w pliku automatycznym
                            last_auto_time = df_auto['time'].max()

                            # Filtruj tylko nowsze dane z pliku manualnego
                            df_new_data = df_manual[df_manual['time'] > last_auto_time]

                            if df_new_data.empty:
                                print(f"ℹ️ Brak nowszych danych w {file} niż w {os.path.basename(auto_path)}")
                                os.remove(manual_path)
                                print(f"🗑️ Usunięto plik manualny: {file}")
                                continue

                            # Połącz dane
                            df_combined = pd.concat([df_auto, df_new_data], ignore_index=True)
                        else:
                            # Jeśli nie ma pliku automatycznego, użyj wszystkich danych manualnych
                            df_combined = df_manual

                        # Posortuj i usuń duplikaty
                        df_combined.sort_values('time', inplace=True)
                        df_combined.drop_duplicates(subset='time', keep='last', inplace=True)

                        # Zapisz wynik zachowując format timestamp
                        df_combined.to_csv(auto_path, index=False)
                        print(f"✅ Zaktualizowano {os.path.basename(auto_path)} danymi z {file}")

                        # Usuń plik manualny
                        os.remove(manual_path)
                        print(f"🗑️ Usunięto plik manualny: {file}")

                    except Exception as e:
                        print(f"❌ Błąd podczas przetwarzania {file}: {str(e)}")


if __name__ == "__main__":
    merge_and_replace_data()