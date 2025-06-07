"""Moduł do analizy sezonowości w danych finansowych."""

from typing import Tuple, Dict, Any, Optional
import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt
import logging
from statsmodels.tsa.stattools import acf, pacf
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from scipy import stats

logger = logging.getLogger(__name__)

class SeasonalAnalyzer:
    """Klasa do analizy sezonowości w szeregach czasowych."""
    
    def __init__(self, period: Optional[int] = None):
        """Inicjalizacja analizatora sezonowości.
        
        Args:
            period: Okres sezonowości (liczba obserwacji w cyklu). 
                   Jeśli None, zostanie oszacowany automatycznie.
        """
        self.period = period
    
    def decompose(
        self, 
        series: pd.Series, 
        model: str = 'additive',
        extrapolate_trend: bool = True
    ) -> Dict[str, pd.Series]:
        """Dekompozycja szeregu czasowego na trend, sezonowość i reszty.
        
        Args:
            series: Szereg czasowy do analizy
            model: Typ modelu ('additive' lub 'multiplicative')
            extrapolate_trend: Czy ekstrapolować trend dla brakujących wartości
            
        Returns:
            Słownik z trendem, sezonowością i resztami
        """
        # Okres sezonowości (domyślnie autowykrywanie)
        period = self.estimate_seasonal_period(series) if self.period is None else self.period
        
        try:
            # Wykonaj dekompozycję
            decomposition = seasonal_decompose(
                series.dropna(),  # Usuń brakujące wartości
                model=model,
                period=period,
                extrapolate_trend=extrapolate_trend
            )
            
            # Przygotuj wyniki
            result = {
                'observed': series,
                'trend': decomposition.trend,
                'seasonal': decomposition.seasonal,
                'resid': decomposition.resid
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Błąd podczas dekompozycji sezonowej: {e}")
            raise
    
    def estimate_seasonal_period(self, series: pd.Series) -> int:
        """Szacuje okres sezonowości na podstawie danych.
        
        Args:
            series: Szereg czasowy
            
        Returns:
            Szacowany okres seasonalności (liczba obserwacji w cyklu)
        """
        # Dla danych dziennych (D1) - tygodniowa sezonowość (5 dni)
        if self._is_daily_data(series):
            return 5  # 5 dni handlowych w tygodniu
        
        # Dla danych godzinowych (H1, H4) - dzienna sezonowość (8 godzin handlowych)
        elif self._is_hourly_data(series):
            return 8  # 8 godzin handlowych w ciągu dnia
        
        # Dla danych minutowych (M1, M5, M15) - dzienna sezonowość (liczba świec w ciągu dnia)
        elif self._is_minute_data(series):
            # Dla M1: 8h * 60min = 480 świec
            # Dla M5: 8h * 12 = 96 świec
            # Dla M15: 8h * 4 = 32 świece
            freq = pd.infer_freq(series.index)
            if 'T' in freq:  # Minutowe dane
                minutes = int(freq.replace('T', ''))
                return (8 * 60) // minutes
            return 96  # Domyślnie dla M5
        
        # Domyślna wartość (tygodniowa sezonowość)
        return 5
    
    def _is_daily_data(self, series: pd.Series) -> bool:
        """Sprawdza, czy dane są dzienne."""
        if len(series) < 2:
            return False
        
        # Sprawdź różnicę czasową między pierwszymi dwoma punktami
        delta = series.index[1] - series.index[0]
        return delta >= pd.Timedelta(days=1)
    
    def _is_hourly_data(self, series: pd.Series) -> bool:
        """Sprawdza, czy dane są godzinowe."""
        if len(series) < 2:
            return False
            
        delta = series.index[1] - series.index[0]
        return pd.Timedelta(hours=1) <= delta < pd.Timedelta(days=1)
    
    def _is_minute_data(self, series: pd.Series) -> bool:
        """Sprawdza, czy dane są minutowe."""
        if len(series) < 2:
            return False
            
        delta = series.index[1] - series.index[0]
        return delta < pd.Timedelta(hours=1)
    
    def plot_decomposition(self, decomposition: Dict[str, pd.Series], figsize=(12, 8)):
        """Wizualizacja dekompozycji szeregu czasowego.
        
        Args:
            decomposition: Wynik metody decompose()
            figsize: Rozmiar wykresu
        """
        plt.figure(figsize=figsize)
        
        # Obserwowane
        plt.subplot(411)
        plt.plot(decomposition['observed'])
        plt.title('Obserwowane')
        plt.grid(True)
        
        # Trend
        plt.subplot(412)
        plt.plot(decomposition['trend'])
        plt.title('Trend')
        plt.grid(True)
        
        # Sezonowość
        plt.subplot(413)
        plt.plot(decomposition['seasonal'])
        plt.title('Sezonowość')
        plt.grid(True)
        
        # Reszty
        plt.subplot(414)
        plt.plot(decomposition['resid'])
        plt.title('Reszty')
        plt.grid(True)
        
        plt.tight_layout()
        plt.show()
    
    def plot_acf_pacf(
        self, 
        series: pd.Series, 
        lags: int = 40, 
        figsize: Tuple[int, int] = (12, 8)
    ):
        """Wykresy ACF i PACF dla analizy autokorelacji.
        
        Args:
            series: Szereg czasowy
            lags: Liczba opóźnień do wyświetlenia
            figsize: Rozmiar wykresu
        """
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=figsize)
        
        # Wykres ACF
        plot_acf(series, lags=lags, ax=ax1)
        ax1.set_title('Funkcja autokorelacji (ACF)')
        
        # Wykres PACF
        plot_pacf(series, lags=lags, ax=ax2, method='ols')
        ax2.set_title('Częściowa funkcja autokorelacji (PACF)')
        
        plt.tight_layout()
        plt.show()
    
    def test_stationarity(self, series: pd.Series, window: int = 12) -> Dict[str, Any]:
        """Testuje stacjonarność szeregu czasowego.
        
        Args:
            series: Szereg czasowy do analizy
            window: Okno do obliczania statystyk kroczących
            
        Returns:
            Słownik z wynikami testów i statystykami
        """
        from statsmodels.tsa.stattools import adfuller
        
        # Oblicz statystyki kroczące
        rolmean = series.rolling(window=window).mean()
        rolstd = series.rolling(window=window).std()
        
        # Wykres statystyk kroczących
        plt.figure(figsize=(12, 6))
        orig = plt.plot(series, color='blue', label='Oryginalne')
        mean = plt.plot(rolmean, color='red', label='Średnia krocząca')
        std = plt.plot(rolstd, color='black', label='Odchylenie standardowe')
        plt.legend(loc='best')
        plt.title('Średnia krocząca i odchylenie standardowe')
        plt.show()
        
        # Test Dickey-Fullera
        dftest = adfuller(series, autolag='AIC')
        dfoutput = pd.Series(
            dftest[0:4],
            index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used']
        )
        
        for key, value in dftest[4].items():
            dfoutput[f'Critical Value ({key})'] = value
        
        return dfoutput.to_dict()


def example_usage():
    """Przykładowe użycie klasy SeasonalAnalyzer."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from .data_loader import DataLoader
    
    # Wczytaj dane
    loader = DataLoader()
    series, _ = loader.prepare_data(
        symbol='US.100+',
        timeframe='M1',
        filter_trading=True
    )
    
    # Analiza sezonowości
    analyzer = SeasonalAnalyzer()
    
    # Test stacjonarności
    print("\nTest stacjonarności:")
    stationarity = analyzer.test_stationarity(series)
    for k, v in stationarity.items():
        print(f"{k}: {v}")
    
    # Dekompozycja szeregu
    print("\nDekompozycja szeregu czasowego...")
    decomposition = analyzer.decompose(series)
    analyzer.plot_decomposition(decomposition)
    
    # Analiza ACF/PACF
    print("\nAnaliza ACF/PACF...")
    analyzer.plot_acf_pacf(series)


if __name__ == "__main__":
    example_usage()
