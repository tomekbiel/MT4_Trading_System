# Analiza szeregów czasowych z użyciem modeli ARIMA/SARIMA

Narzędzie do analizy i prognozowania szeregów czasowych, zaprojektowane specjalnie dla danych finansowych, z uwzględnieniem specyfiki rynków finansowych (godziny handlu, dni wolne, sezonowość).

## Funkcjonalności

- **Ładowanie i przygotowanie danych**
  - Obsługa różnych interwałów czasowych (M1, M5, M15, H1, H4, D1)
  - Automatyczne wykrywanie i obsługa brakujących danych
  - Filtrowanie godzin handlowych

- **Analiza eksploracyjna**
  - Wykresy szeregów czasowych
  - Analiza stacjonarności (test Dickey-Fullera)
  - Dekompozycja na trend, sezonowość i reszty
  - Analiza autokorelacji (ACF/PACF)

- **Modelowanie ARIMA/SARIMA**
  - Automatyczne znajdowanie optymalnych parametrów (p,d,q)(P,D,Q)m
  - Walidacja krzyżowa
  - Ocena jakości prognozy (MSE, RMSE, MAE, MAPE)
  - Wizualizacja wyników

- **Dodatkowe funkcje**
  - Obsługa zmiennych egzogenicznych
  - Analiza reszt
  - Eksport wyników do plików (CSV, PNG, JSON)

## Wymagania

- Python 3.8+
- Biblioteki wymienione w pliku `requirements.txt`

## Instalacja

1. Sklonuj repozytorium:
   ```bash
   git clone https://github.com/TwojaNazwaUzytkownika/MT4_Trading_System.git
   cd MT4_Trading_System
   ```

2. Zainstaluj wymagane pakiety:
   ```bash
   pip install -r requirements.txt
   ```

## Szybki start

### Użycie z wiersza poleceń

```bash
# Analiza dla US.100+ na interwale M1 (minutowym)
python -m tools.arima_analyzer --symbol US.100+ --timeframe M1 --output-dir results

# Analiza dla EURUSD+ na interwale H1 (godzinnym) z określonym zakresem dat
python -m tools.arima_analyzer --symbol EURUSD+ --timeframe H1 --start-date 2023-01-01 --end-date 2023-12-31

# Analiza bez uwzględniania sezonowości
python -m tools.arima_analyzer --symbol GOLDs+ --timeframe D1 --no-seasonal
```

### Użycie z kodu Pythona

```python
from tools.arima_analyzer import run_analysis

# Przeprowadź analizę
results = run_analysis(
    symbol='US.100+',
    timeframe='M1',
    start_date='2023-01-01',
    end_date='2023-12-31',
    train_size=0.8,
    output_dir='results',
    log_level='INFO',
    seasonal=True
)

# Wyświetl metryki jakości prognozy
print("Metryki jakości prognozy:")
for metric, value in results['metrics'].items():
    print(f"{metric.upper()}: {value:.4f}")
```

## Struktura projektu

```
arima_analyzer/
├── __init__.py           # Inicjalizacja pakietu
├── __main__.py           # Główny skrypt uruchomieniowy
├── arima_model.py        # Implementacja modelu ARIMA/SARIMA
├── config.py             # Konfiguracja
├── data_loader.py        # Ładowanie i przygotowanie danych
├── seasonal_decompose.py # Analiza sezonowości
└── utils.py             # Funkcje pomocnicze
```

## Przykładowy wynik analizy

### Wykres prognozy

![Przykładowy wykres prognozy](example_forecast.png)

### Dekompozycja sezonowa

![Dekompozycja sezonowa](example_decomposition.png)

### Analiza ACF/PACF

![Analiza ACF/PACF](example_acf_pacf.png)

## Dostosowywanie

### Konfiguracja

Główne parametry konfiguracyjne znajdują się w pliku `config.py`. Możesz dostosować:
- Domyślne parametry modelu ARIMA
- Godziny handlu
- Ścieżki do katalogów z danymi i wynikami
- Ustawienia logowania

### Dodawanie własnych wskaźników

Możesz rozszerzyć funkcjonalność o własne wskaźniki techniczne, modyfikując funkcję `add_technical_indicators` w pliku `utils.py`.

## Licencja

[MIT](LICENSE)

## Autor

Twój projekt MT4 Trading System
