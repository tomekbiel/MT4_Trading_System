# 1. Import niezbędnych bibliotek
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# 2. Wczytanie danych
file_path = r'C:\python\MT4_Trading_System\data\historical\US.100+\M1\US.100+_M1.csv'
df = pd.read_csv(file_path)

# 3. Podstawowe informacje o danych
print("=== PODSTAWOWE INFORMACJE ===")
print(df.info())

# 4. Podgląd pierwszych i ostatnich 5 wierszy
print("\n=== PIERWSZE 5 WIERSZY ===")
print(df.head())
print("\n=== OSTATNIE 5 WIERSZY ===")
print(df.tail())

# 5. Sprawdzenie braków danych
print("\n=== BRAKI DANYCH ===")
print(df.isnull().sum())

# 6. Konwersja daty na format datetime (jeśli kolumna z datą istnieje)
if 'time' in df.columns:
    df['time'] = pd.to_datetime(df['time'])
    print("\n=== ZAKRES CZASOWY DANYCH ===")
    print(f"Od: {df['time'].min()}")
    print(f"Do: {df['time'].max()}")
    print(f"Liczba unikalnych dat: {df['time'].dt.date.nunique()}")

# 7. Analiza wartości numerycznych
print("\n=== STATYSTYKI OPISOWE ===")
print(df.describe())

# 8. Sprawdzenie duplikatów
print("\n=== LICZBA DUPLIKATÓW ===")
print(f"Liczba zduplikowanych wierszy: {df.duplicated().sum()}")

# 9. Analiza zmienności cen
if all(col in df.columns for col in ['open', 'high', 'low', 'close']):
    print("\n=== ANALIZA ZMIENNOŚCI CEN ===")
    df['returns'] = df['close'].pct_change()
    print("\nStatystyki stóp zwrotu:")
    print(df['returns'].describe())

# 10. Wizualizacja danych
if 'close' in df.columns:
    print("\nRysowanie wykresu ceny zamknięcia...")
    plt.figure(figsize=(12, 6))
    df.set_index('time')['close'].plot()
    plt.title('Wykres ceny zamknięcia US.100+')
    plt.xlabel('Data')
    plt.ylabel('Cena zamknięcia')
    plt.grid(True)
    plt.show()