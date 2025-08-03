import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
from typing import Optional

warnings.filterwarnings('ignore')

class ARIMA:
    def __init__(self,datos:pd.DataFrame):
        self.data = datos

    def stationary(self, columna='Adj Close',data01: Optional[pd.DataFrame] = None,counter: Optional[int] = 0):
        """Analiza si la serie es estacionaria"""

        print(f"\n=== ANÁLISIS DE ESTACIONARIEDAD ({columna}) ===")

        datos_trabajo = data01.copy() if data01 is not None else self.data.copy()

        serie = datos_trabajo.dropna()

        # Test de Dickey-Fuller
        resultado_adf = adfuller(serie)

        print(f"Estadístico ADF: {resultado_adf[0]:.6f}")
        print(f"p-valor: {resultado_adf[1]:.6f}")
        print(f"Valores críticos:")
        for clave, valor in resultado_adf[4].items():
            print(f"\t{clave}: {valor:.3f}")

        # Interpretación
        if resultado_adf[1] <= 0.05 and counter < 5:
            print(f"✅ La serie ES estacionaria (p-valor <= 0.05) {counter}")
            print(f"Número de diferencias necesarias para la estacionaridad: {counter}")
            return datos_trabajo
        else:
            print(f"❌ La serie NO es estacionaria (p-valor > 0.05) {counter}")
            dataset = self.make_stationary(serie)
            counter = counter + 1
            return self.stationary(columna = "Adj Close",data01 = dataset,counter = counter)



    def make_stationary(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Convierte la serie en estacionaria usando diferenciación"""
        print(f"\n=== HACIENDO SERIE ESTACIONARIA ===")

        # Primera diferenciación
        serie_diff = data.diff().dropna()

        return serie_diff


