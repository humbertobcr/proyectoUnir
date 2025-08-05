import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
from typing import Optional
from statsmodels.tsa.stattools import adfuller, acf, pacf
import numpy as np
import itertools
from statsmodels.tsa.statespace.sarimax import SARIMAX
import warnings
import pmdarima as pm
from joblib import Parallel, delayed
import time

warnings.filterwarnings('ignore')

class SARIMAXmodel:
    def __init__(self,datos:pd.DataFrame,ticker:str):
        self.data = datos
        self.ticker = ticker
        self.modelo = None

    def encontrar_s_optimo(self, periodos_a_probar=[5, 21, 63, 126, 252]):
        """
        Analiza la serie para encontrar el período estacional 's' más probable.

        Args:
            serie (pd.Series): La serie de tiempo original.
            periodos_a_probar (list): Lista de períodos 's' candidatos.

        Returns:
            int: El valor de 's' que muestra la autocorrelación más fuerte, o None si no hay
                 una estacionalidad clara.
        """
        serie = self.data
        print("\n" + "=" * 70)
        print("BUSCANDO EL PERÍODO ESTACIONAL ÓPTIMO ('s')")
        print("=" * 70)

        mejor_s = None
        max_autocorr = 0.0

        try:
            # Calcular la autocorrelación para todos los lags necesarios
            max_lag = max(periodos_a_probar) + 5
            autocorrelaciones = acf(serie.dropna(), nlags=max_lag, fft=True)

            print("Analizando la fuerza de la autocorrelación en los lags estacionales:")

            for s in periodos_a_probar:
                if s < len(autocorrelaciones):
                    # Tomamos el valor absoluto de la autocorrelación en el lag 's'
                    autocorr_en_s = abs(autocorrelaciones[s])
                    print(f"  - s = {s:<3}: Autocorrelación = {autocorr_en_s:.4f}")

                    # Si esta autocorrelación es la más fuerte hasta ahora, la guardamos
                    if autocorr_en_s > max_autocorr:
                        max_autocorr = autocorr_en_s
                        mejor_s = s

        except Exception as e:
            print(f"Error durante la búsqueda de s: {e}")
            return None

        # Umbral mínimo: si la autocorrelación más fuerte es muy débil,
        # consideramos que no hay estacionalidad.
        umbral_minimo = 0.2
        if max_autocorr < umbral_minimo:
            print(f"\nNo se encontró una estacionalidad clara (autocorrelación máxima < {umbral_minimo}).")
            return None

        print(f"\n✅ 's' óptimo recomendado: {mejor_s} (con autocorrelación de {max_autocorr:.4f})")
        return mejor_s

    def encontrar_mejor_sarimax_rapido(self):
        """
        Usa auto_arima para encontrar el mejor modelo SARIMAX para una sola acción.
        Es mucho más rápido que una búsqueda exhaustiva.
        """
        ticker = self.ticker
        serie = self.data
        print(f"🚀 Iniciando búsqueda para {ticker}...")
        try:
            # auto_arima encuentra los mejores p,d,q,P,D,Q automáticamente.
            # Le damos el 's' (s_optimo) que ya conocemos o sospechamos.
            s_optimo = self.encontrar_s_optimo()  # Asumimos mensual como un buen candidato general

            modelo_auto = pm.auto_arima(
                serie,
                start_p=1, start_q=1,
                test='adf',  # Usa el test ADF para encontrar 'd'
                max_p=3, max_q=3,
                m=s_optimo,  # Aquí se define 's'
                start_P=0,
                seasonal=True,  # Activa la búsqueda estacional
                d=None,  # Permite que la librería encuentre 'd'
                D=None,  # Permite que la librería encuentre 'D'
                trace=False,  # No imprimir cada paso
                error_action='ignore',
                suppress_warnings=True,
                stepwise=True  # Búsqueda escalonada (mucho más rápida)
            )

            print(f"✅ Mejor modelo para {ticker}: {modelo_auto.order} {modelo_auto.seasonal_order}")
            return {
                'ticker': ticker,
                'aic': modelo_auto.aic(),
                'order': modelo_auto.order,
                'seasonal_order': modelo_auto.seasonal_order
            }
        except Exception as e:
            print(f"❌ Error con {ticker}: {e}")
            return {'ticker': ticker, 'error': str(e)}

    def pronosticar_sarimax(self,periodos_a_predecir) -> pd.DataFrame:
        """
        Crea, entrena y pronostica con un modelo SARIMAX dado los parámetros.

        Args:
            datos (pd.Series): La serie de tiempo original para el entrenamiento.
            order (tuple): Tupla (p, d, q) del componente no estacional.
            seasonal_order (tuple): Tupla (P, D, Q, s) del componente estacional.
            periodos_a_predecir (int): Número de períodos futuros a pronosticar.

        Returns:
            pd.DataFrame: Un DataFrame con las predicciones, límites inferiores
                          y superiores del intervalo de confianza.
        """
        datos = self.data
        parametros = self.encontrar_mejor_sarimax_rapido()
        order = parametros["order"]
        seasonal_order = parametros["seasonal_order"]

        print("📋 Parámetros de SARIMAX a utilizar:")
        print(f"  - No estacional (p, d, q): {order}")
        print(f"  - Estacional (P, D, Q, s): {seasonal_order}")
        print("-" * 50)

        try:
            # 1. Instanciar y entrenar el modelo SARIMAX de statsmodels
            # El argumento `enforce_stationarity=False` puede ser útil
            # si `auto_arima` encuentra `d > 0`.
            modelo = SARIMAX(
                datos,
                order=order,
                seasonal_order=seasonal_order,
                enforce_stationarity=False,
                enforce_invertibility=False
            )

            print("🔄 Entrenando el modelo...")
            resultados = modelo.fit(disp=False)
            print("✅ Modelo entrenado exitosamente.")

            # 2. Generar predicciones
            print(f"🔮 Generando predicciones para los próximos {periodos_a_predecir} períodos...")

            # El método get_forecast() es más robusto para generar predicciones
            # y sus intervalos de confianza.
            pronostico_resultados = resultados.get_forecast(steps=periodos_a_predecir)

            # Extraer los pronósticos y los intervalos de confianza
            pronosticos = pronostico_resultados.predicted_mean
            intervalos_confianza = pronostico_resultados.conf_int()

            # Unir todos los resultados en un solo DataFrame
            df_predicciones = pd.DataFrame({
                'pronostico': pronosticos,
                'limite_inferior': intervalos_confianza['lower Adj Close'],
                'limite_superior': intervalos_confianza['upper Adj Close']
            })

            print("✅ Pronósticos y límites generados.")
            return df_predicciones

        except Exception as e:
            print(f"❌ Ocurrió un error durante el pronóstico: {e}")
            return None


