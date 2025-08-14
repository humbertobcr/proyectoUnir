import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
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
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')

class SARIMAXmodel:
    def __init__(self,datos:pd.DataFrame,ticker:str):
        self.data = datos
        self.serie = self.data["Adj Close"]
        self.ticker = ticker
        self.modelo = None
        self.resultados = None
        self.periods = None
        self.df_predicciones = None

    def encontrar_s_optimo(self, periodos_a_probar=[5, 7, 21, 63, 126, 252]):
        """
        Analiza la serie para encontrar el período estacional 's' más probable.

        Args:
            serie (pd.Series): La serie de tiempo original.
            periodos_a_probar (list): Lista de períodos 's' candidatos.

        Returns:
            int: El valor de 's' que muestra la autocorrelación más fuerte, o None si no hay
                 una estacionalidad clara.
        """
        serie = self.serie
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
        serie = self.serie
        print(f"🚀 Iniciando búsqueda para {ticker}...")
        try:
            # auto_arima encuentra los mejores p,d,q,P,D,Q automáticamente.
            # Le damos el 's' (s_optimo) que ya conocemos o sospechamos.
            s_optimo = self.encontrar_s_optimo()  # Asumimos mensual como un buen candidato general

            modelo_auto = pm.auto_arima(
                serie,
                start_p=1, start_q=1,
                test='adf',  # Usa el test ADF para encontrar 'd'
                max_p=4, max_q=4,
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
        datos = self.serie
        parametros = self.encontrar_mejor_sarimax_rapido()
        order = parametros["order"]
        seasonal_order = parametros["seasonal_order"]
        self.periods =periodos_a_predecir

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
            self.resultados = modelo.fit(disp=False)
            print("✅ Modelo entrenado exitosamente.")

            # 2. Generar predicciones
            print(f"🔮 Generando predicciones para los próximos {periodos_a_predecir} períodos...")

            # El método get_forecast() es más robusto para generar predicciones
            # y sus intervalos de confianza.
            pronostico_resultados = self.resultados.get_forecast(steps=periodos_a_predecir)

            # Extraer los pronósticos y los intervalos de confianza
            pronosticos = pronostico_resultados.predicted_mean
            intervalos_confianza = pronostico_resultados.conf_int()

            # Unir todos los resultados en un solo DataFrame


            last_date = self.data['Date'].iloc[-1]

            datesPredicted = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=self.periods)

            self.df_predicciones = pd.DataFrame({
                'Date':datesPredicted,
                'frcst_sarimax_01_mean': pronosticos,
                'frcst_sarimax_01_low': intervalos_confianza['lower Adj Close'],
                'frcst_sarimax_01_upper': intervalos_confianza['upper Adj Close']
            })

            df_final = pd.concat([self.data, self.df_predicciones])

            print("✅ Pronósticos y límites generados.")
            return df_final

        except Exception as e:
            print(f"❌ Ocurrió un error durante el pronóstico: {e}")
            return None

    def evaluar_modelo(self):
        """
        Evalúa el modelo SARIMAX usando datos de entrenamiento (in-sample).
        """
        if self.resultados is None:
            print("⚠️ Primero entrena el modelo usando 'pronosticar_sarimax()'")
            return None

        # Predicción dentro de la muestra
        predicciones_in_sample = self.resultados.predict(start=0, end=len(self.serie)-1)
        reales = self.serie

        # Calcular métricas
        mae = mean_absolute_error(reales, predicciones_in_sample)
        rmse = np.sqrt(mean_squared_error(reales, predicciones_in_sample))
        mape = np.mean(np.abs((reales - predicciones_in_sample) / reales)) * 100
        r2 = r2_score(reales, predicciones_in_sample)

        print("\n📊 Evaluación del Modelo (In-sample):")
        print(f"  - AIC:  {self.resultados.aic:.2f}")
        print(f"  - BIC:  {self.resultados.bic:.2f}")
        print(f"  - Mean Absolute Error:  {mae:.4f}")
        print(f"  - Root Mean Squared Error: {rmse:.4f}")
        print(f"  - Mean Absolute Porcentual Error: {mape:.2f}%")
        print(f"  - R²:   {r2:.4f}")

        return {
            'AIC': self.resultados.aic,
            'BIC': self.resultados.bic,
            'MAE': mae,
            'RMSE': rmse,
            'MAPE': mape,
            'R2': r2
        }

    def graficar_ajuste(self, periodos_a_predecir=10):
        if self.resultados is None:
            print("⚠️ Primero entrena el modelo con 'pronosticar_sarimax()'")
            return

        df_final = pd.concat([self.data,self.df_predicciones])

        # Valores reales
        reales = self.serie
        # Ajuste in-sample
        fitted = self.resultados.fittedvalues

        # Pronóstico futuro
        forecast_result = self.resultados.get_forecast(steps=periodos_a_predecir)
        predicciones_futuro = forecast_result.predicted_mean
        intervalos_confianza = forecast_result.conf_int()

        plt.figure(figsize=(12, 6))

        lenL = len(self.data)
        lenU = len(self.data)+periodos_a_predecir

        # Serie real
        plt.plot(self.data['Date'], reales, label='Real', color='black', linewidth=1.5)
        # Ajuste del modelo
        plt.plot(self.data['Date'], fitted, label='Ajuste modelo (in-sample)', color='red', linestyle='--')
        # Pronóstico
        plt.plot(df_final.iloc[lenL:lenU]['Date'], predicciones_futuro, label='Pronóstico', color='blue')
        # Intervalos de confianza
        plt.fill_between(
            df_final.iloc[lenL:lenU]['Date'],
            intervalos_confianza.iloc[:, 0],
            intervalos_confianza.iloc[:, 1],
            color='blue',
            alpha=0.2,
            label='Intervalo de confianza'
        )

        plt.title(f"Ajuste y Pronóstico SARIMAX - {self.ticker}")
        plt.xlabel("Fecha")
        plt.ylabel("Valor")
        plt.legend()
        plt.grid(True)
        plt.show()




