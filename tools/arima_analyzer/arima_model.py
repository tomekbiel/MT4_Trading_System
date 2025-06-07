"""Moduł do modelowania ARIMA dla danych finansowych."""

import warnings
from typing import Dict, Tuple, List, Optional, Union
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA as ARIMAModel
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
import pmdarima as pm
import logging

from .config import DEFAULT_TRAIN_SIZE, DEFAULT_ARIMA_ORDER

logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


class ARIMAAnalyzer:
    """Klasa do analizy i prognozowania szeregów czasowych przy użyciu modeli ARIMA."""
    
    def __init__(self, seasonal: bool = True, m: int = None):
        """Inicjalizacja analizatora ARIMA.
        
        Args:
            seasonal: Czy uwzględniać sezonowość (SARIMA)
            m: Okres sezonowości (liczba obserwacji w cyklu)
        """
        self.seasonal = seasonal
        self.m = m
        self.model = None
        self.model_fit = None
        self.best_order = None
        self.best_seasonal_order = None
    
    def find_best_arima(
        self, 
        series: pd.Series,
        seasonal: bool = None,
        m: int = None,
        test: str = 'aic',
        trace: bool = True,
        suppress_warnings: bool = True,
        stepwise: bool = True,
        **kwargs
    ) -> Dict:
        """Znajduje najlepsze parametry modelu ARIMA/SARIMA.
        
        Args:
            series: Szereg czasowy do analizy
            seasonal: Czy uwzględniać sezonowość
            m: Okres sezonowości
            test: Kryterium wyboru modelu ('aic', 'bic', 'aicc', 'hqic')
            trace: Czy wyświetlać postęp
            suppress_warnings: Czy wyciszać ostrzeżenia
            stepwise: Czy używać metody krokowej do znajdowania parametrów
            **kwargs: Dodatkowe argumenty dla auto_arima
            
        Returns:
            Słownik z najlepszymi parametrami i metrykami
        """
        seasonal = self.seasonal if seasonal is None else seasonal
        m = self.m if m is None else m
        
        logger.info("Szukanie najlepszych parametrów ARIMA...")
        
        try:
            model = pm.auto_arima(
                series,
                seasonal=seasonal,
                m=m,
                test=test,
                trace=trace,
                suppress_warnings=suppress_warnings,
                stepwise=stepwise,
                **kwargs
            )
            
            self.model = model
            self.best_order = model.order
            self.best_seasonal_order = model.seasonal_order if hasattr(model, 'seasonal_order') else None
            
            result = {
                'order': model.order,
                'seasonal_order': model.seasonal_order if hasattr(model, 'seasonal_order') else None,
                'aic': model.aic(),
                'bic': model.bic(),
                'aicc': model.aicc(),
                'hqic': model.hqic(),
                'model': model
            }
            
            logger.info(f"Znaleziono najlepsze parametry: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Błąd podczas znajdowania najlepszych parametrów ARIMA: {e}")
            raise
    
    def fit(
        self, 
        series: pd.Series,
        order: Tuple[int, int, int] = None,
        seasonal_order: Tuple[int, int, int, int] = None,
        **kwargs
    ) -> Dict:
        """Dopasowuje model ARIMA/SARIMA do danych.
        
        Args:
            series: Szereg czasowy do dopasowania
            order: Krotka (p,d,q) dla ARIMA
            seasonal_order: Krotka (P,D,Q,m) dla sezonowości
            **kwargs: Dodatkowe argumenty dla modelu
            
        Returns:
            Słownik z wynikami dopasowania
        """
        order = order or self.best_order or DEFAULT_ARIMA_ORDER
        
        try:
            if self.seasonal and (seasonal_order or self.best_seasonal_order):
                # Użyj SARIMAX dla modeli sezonowych
                seasonal_order = seasonal_order or self.best_seasonal_order
                logger.info(f"Dopasowywanie modelu SARIMA{order}x{seasonal_order}...")
                self.model = SARIMAX(
                    series,
                    order=order,
                    seasonal_order=seasonal_order,
                    **kwargs
                )
            else:
                # Użyj standardowego ARIMA dla modeli bez sezonowości
                logger.info(f"Dopasowywanie modelu ARIMA{order}...")
                self.model = ARIMAModel(
                    series,
                    order=order,
                    **kwargs
                )
            
            # Dopasuj model
            self.model_fit = self.model.fit()
            logger.info("Model pomyślnie dopasowany")
            
            # Podsumowanie modelu
            print(self.model_fit.summary())
            
            return {
                'order': order,
                'seasonal_order': seasonal_order if self.seasonal else None,
                'aic': self.model_fit.aic,
                'bic': self.model_fit.bic,
                'hqic': self.model_fit.hqic,
                'resid': self.model_fit.resid,
                'model': self.model_fit
            }
            
        except Exception as e:
            logger.error(f"Błąd podczas dopasowywania modelu: {e}")
            raise
    
    def forecast(
        self, 
        steps: int = 1,
        exog: Optional[pd.DataFrame] = None,
        alpha: float = 0.05,
        return_conf_int: bool = True
    ) -> Dict:
        """Prognozuje przyszłe wartości.
        
        Args:
            steps: Liczba kroków do przodu
            exog: Zmienne egzogeniczne (opcjonalne)
            alpha: Poziom istotności dla przedziałów ufności
            return_conf_int: Czy zwracać przedziały ufności
            
        Returns:
            Słownik z prognozą i przedziałami ufności
        """
        if self.model_fit is None:
            raise ValueError("Najpierw należy dopasować model (użyj metody fit)")
        
        try:
            # Prognoza
            if hasattr(self.model_fit, 'get_forecast'):
                # Dla SARIMAX
                forecast_result = self.model_fit.get_forecast(steps=steps, exog=exog)
                forecast = forecast_result.predicted_mean
                
                if return_conf_int:
                    conf_int = forecast_result.conf_int(alpha=alpha)
                else:
                    conf_int = None
            else:
                # Dla ARIMA
                forecast, stderr, conf_int = self.model_fit.forecast(
                    steps=steps, 
                    exog=exog,
                    alpha=alpha
                )
                
                if not return_conf_int:
                    conf_int = None
            
            result = {'forecast': forecast}
            
            if conf_int is not None:
                result['conf_int'] = conf_int
            
            return result
            
        except Exception as e:
            logger.error(f"Błąd podczas prognozowania: {e}")
            raise
    
    def evaluate(
        self, 
        y_true: Union[pd.Series, np.ndarray], 
        y_pred: Union[pd.Series, np.ndarray]
    ) -> Dict[str, float]:
        """Ocenia jakość prognozy.
        
        Args:
            y_true: Rzeczywiste wartości
            y_pred: Prognozowane wartości
            
        Returns:
            Słownik z metrykami jakości
        """
        y_true = np.asarray(y_true)
        y_pred = np.asarray(y_pred)
        
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(y_true, y_pred)
        mape = mean_absolute_percentage_error(y_true, y_pred) * 100  # w procentach
        
        return {
            'mse': mse,
            'rmse': rmse,
            'mae': mae,
            'mape': mape
        }
    
    def plot_forecast(
        self, 
        train: pd.Series,
        test: Optional[pd.Series] = None,
        forecast: Optional[Union[pd.Series, np.ndarray]] = None,
        conf_int: Optional[np.ndarray] = None,
        figsize: Tuple[int, int] = (15, 6)
    ) -> None:
        """Wizualizuje dane treningowe, testowe i prognozę.
        
        Args:
            train: Dane treningowe
            test: Dane testowe (opcjonalne)
            forecast: Prognoza
            conf_int: Przedziały ufności (dolna i górna granica)
            figsize: Rozmiar wykresu
        """
        plt.figure(figsize=figsize)
        
        # Rysuj dane treningowe
        plt.plot(train.index, train, label='Dane treningowe', color='blue')
        
        # Rysuj dane testowe, jeśli podane
        if test is not None:
            plt.plot(test.index, test, label='Dane testowe', color='green')
        
        # Rysuj prognozę, jeśli podana
        if forecast is not None:
            forecast_index = pd.date_range(
                start=train.index[-1] + (train.index[1] - train.index[0]),
                periods=len(forecast),
                freq=train.index.freq or 'D'
            )
            
            plt.plot(forecast_index, forecast, label='Prognoza', color='red')
            
            # Rysuj przedziały ufności, jeśli podane
            if conf_int is not None:
                plt.fill_between(
                    forecast_index,
                    conf_int[:, 0],
                    conf_int[:, 1],
                    color='pink',
                    alpha=0.3,
                    label='95% przedział ufności'
                )
        
        plt.title('Prognoza ARIMA')
        plt.xlabel('Data')
        plt.ylabel('Wartość')
        plt.legend()
        plt.grid(True)
        plt.show()


def train_test_split(series: pd.Series, test_size: float = 0.2) -> Tuple[pd.Series, pd.Series]:
    """Dzieli szereg czasowy na zbiór treningowy i testowy.
    
    Args:
        series: Szereg czasowy do podziału
        test_size: Proporcja danych testowych (0-1)
        
    Returns:
        Krotka (train, test)
    """
    size = int(len(series) * (1 - test_size))
    return series[:size], series[size:]


def example_usage():
    """Przykładowe użycie klasy ARIMAAnalyzer."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from .data_loader import DataLoader
    from .seasonal_decompose import SeasonalAnalyzer
    
    # Wczytaj dane
    loader = DataLoader()
    series, _ = loader.prepare_data(
        symbol='US.100+',
        timeframe='M1',
        filter_trading=True
    )
    
    # Analiza sezonowości
    seasonal_analyzer = SeasonalAnalyzer()
    decomposition = seasonal_analyzer.decompose(series)
    seasonal_analyzer.plot_decomposition(decomposition)
    
    # Podział na zbiór treningowy i testowy
    train, test = train_test_split(series, test_size=0.2)
    
    # Inicjalizacja modelu
    model = ARIMAAnalyzer(seasonal=True, m=96)  # 96 = 8h * 12 (dla M5)
    
    # Automatyczne znajdowanie najlepszych parametrów
    best_params = model.find_best_arima(
        train,
        seasonal=True,
        m=96,  # 8h * 12 (dla M5)
        trace=True,
        error_action='ignore',
        suppress_warnings=True,
        stepwise=True
    )
    
    # Dopasowanie modelu
    model_fit = model.fit(train)
    
    # Prognoza
    forecast_steps = len(test)
    forecast_result = model.forecast(steps=forecast_steps)
    
    # Ocena modelu
    metrics = model.evaluate(test, forecast_result['forecast'])
    print("\nMetryki jakości prognozy:")
    for name, value in metrics.items():
        print(f"{name.upper()}: {value:.4f}")
    
    # Wizualizacja
    model.plot_forecast(
        train=train,
        test=test,
        forecast=forecast_result['forecast'],
        conf_int=forecast_result.get('conf_int')
    )


if __name__ == "__main__":
    example_usage()
