import pandas as pd
from pandas import DataFrame
import numpy as np
import ta
from typing import Optional

class tecnicalAnalysis:
    def __init__(self,datos:DataFrame):
        # Guardar referencia a datos originales (sin modificar)
        self.data_original = datos

    def indctr_01_roi(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Rendimiento aritmético (simple)
        datos_trabajo['rendimiento_aritmetico'] = datos_trabajo['Adj Close'].pct_change()

        # Rendimiento logarítmico
        datos_trabajo['rendimiento_logaritmico'] = np.log(datos_trabajo['Adj Close'] / datos_trabajo['Adj Close'].shift(1))

        # Rendimiento acumulado aritmético
        datos_trabajo['rendimiento_acumulado_arit'] = (1 + datos_trabajo['rendimiento_aritmetico']).cumprod() - 1

        # Rendimiento acumulado logarítmico
        datos_trabajo['rendimiento_acumulado_log'] = datos_trabajo['rendimiento_logaritmico'].cumsum()

        return datos_trabajo

    def indctr_02_volatility(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        datos_trabajo['rendimiento_logaritmico'] = np.log(datos_trabajo['Adj Close'] / datos_trabajo['Adj Close'].shift(1))

        # Volatilidad móvil (desviación estándar de 005 días)
        datos_trabajo['volatilidad_005d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=5).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 010 días)
        datos_trabajo['volatilidad_010d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=10).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 015 días)
        datos_trabajo['volatilidad_015d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=15).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 020 días)
        datos_trabajo['volatilidad_020d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=20).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 030 días)
        datos_trabajo['volatilidad_030d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=30).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 060 días)
        datos_trabajo['volatilidad_060d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=60).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 090 días)
        datos_trabajo['volatilidad_090d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=90).std() * np.sqrt(252)  # Anualizada

        # Volatilidad móvil (desviación estándar de 252 días)
        datos_trabajo['volatilidad_252d'] = datos_trabajo['rendimiento_logaritmico'].rolling(window=252).std() * np.sqrt(252)  # Anualizada

        return datos_trabajo

    def indctr_03_moving_average_exp(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Medias móviles de 5 días de corto plazo
        datos_trabajo['MA005'] = datos_trabajo['Adj Close'].rolling(window=5).mean()
        # Medias móviles de 10 días de corto plazo
        datos_trabajo['MA010'] = datos_trabajo['Adj Close'].rolling(window=10).mean()
        # Medias móviles de 12 días de corto plazo
        datos_trabajo['MA012'] = datos_trabajo['Adj Close'].rolling(window=12).mean()
        # Medias móviles de 20 días de mediano plazo
        datos_trabajo['MA020'] = datos_trabajo['Adj Close'].rolling(window=20).mean()
        # Medias móviles de 50 días de mediano plazo
        datos_trabajo['MA050'] = datos_trabajo['Adj Close'].rolling(window=50).mean()
        # Medias móviles de 60 días de mediano plazo
        datos_trabajo['MA060'] = datos_trabajo['Adj Close'].rolling(window=60).mean()
        # Medias móviles de 100 días de largo plazo
        datos_trabajo['MA100'] = datos_trabajo['Adj Close'].rolling(window=100).mean()
        # Medias móviles de 200 días de largo plazo
        datos_trabajo['MA200'] = datos_trabajo['Adj Close'].rolling(window=200).mean()

        return datos_trabajo

    def indctr_04_moving_average_ar(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Medias móviles exponenciales de 5 días de corto plazo
        datos_trabajo['EMA005'] = datos_trabajo['Adj Close'].ewm(span=5, adjust=False).mean()
        # Medias móviles exponenciales de 10 días de corto plazo
        datos_trabajo['EMA010'] = datos_trabajo['Adj Close'].ewm(span=10, adjust=False).mean()
        # Medias móviles exponenciales de 12 días de corto plazo
        datos_trabajo['EMA012'] = datos_trabajo['Adj Close'].ewm(span=12, adjust=False).mean()
        # Medias móviles exponenciales de 20 días de mediano plazo
        datos_trabajo['EMA020'] = datos_trabajo['Adj Close'].ewm(span=20, adjust=False).mean()
        # Medias móviles exponenciales de 26 días de mediano plazo
        datos_trabajo['EMA026'] = datos_trabajo['Adj Close'].ewm(span=26, adjust=False).mean()
        # Medias móviles exponenciales de 50 días de mediano plazo
        datos_trabajo['EMA050'] = datos_trabajo['Adj Close'].ewm(span=50, adjust=False).mean()
        # Medias móviles exponenciales de 60 días de mediano plazo
        datos_trabajo['EMA060'] = datos_trabajo['Adj Close'].ewm(span=60, adjust=False).mean()
        # Medias móviles exponenciales de 100 días de largo plazo
        datos_trabajo['EMA100'] = datos_trabajo['Adj Close'].ewm(span=100, adjust=False).mean()
        # Medias móviles exponenciales de 200 días de largo plazo
        datos_trabajo['EMA200'] = datos_trabajo['Adj Close'].ewm(span=200, adjust=False).mean()

        return datos_trabajo

    def indctr_05_trend_indicatos(self, data: Optional[pd.DataFrame] = None) -> pd.DataFrame:

        datos_trabajo = data.copy() if data is not None else self.data_original.copy()

        # Medias móviles exponenciales de 26 días de mediano plazo
        datos_trabajo['EMA026'] = datos_trabajo['Adj Close'].ewm(span=26, adjust=False).mean()

        # Medias móviles exponenciales de 12 días de corto plazo
        datos_trabajo['EMA012'] = datos_trabajo['Adj Close'].ewm(span=12, adjust=False).mean()

        datos_trabajo['MACD'] = datos_trabajo['EMA012'] - datos_trabajo['EMA026']
        datos_trabajo['Signal_MACD'] = datos_trabajo['MACD'].ewm(span=9, adjust=False).mean()

        # Calcular ADX, +DI y -DI
        datos_trabajo['ADX'] = ta.trend.adx(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14)
        datos_trabajo['ADX_DI_plus'] = ta.trend.adx_pos(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14)
        datos_trabajo['ADX_DI_less'] = ta.trend.adx_neg(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14)

        # Calcular el RSI (por ejemplo, de 14 días)
        datos_trabajo['RSI'] = ta.momentum.rsi(datos_trabajo['Adj Close'], window=14)

        # Calcular %K y %D
        datos_trabajo['stoch_k'] = ta.momentum.stoch(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14, smooth_window=3)
        datos_trabajo['stoch_d'] = ta.momentum.stoch_signal(datos_trabajo['High'], datos_trabajo['Low'], datos_trabajo['Adj Close'], window=14, smooth_window=3)

        # Calcular la diferencia
        datos_trabajo['stoch_diff'] = datos_trabajo['stoch_k'] - datos_trabajo['stoch_d']

        # Calcular el CCI
        datos_trabajo['cci'] = ta.trend.cci(high=datos_trabajo['High'], low=datos_trabajo['Low'], close=datos_trabajo['Close'], window=20)

        # Calcular Bandas de Bollinger (20-periodos, 2 desviaciones)
        bb = ta.volatility.BollingerBands(close=datos_trabajo['Close'], window=20, window_dev=2)
        datos_trabajo['bb_upper'] = bb.bollinger_hband()
        datos_trabajo['bb_lower'] = bb.bollinger_lband()
        datos_trabajo['bb_middle'] = bb.bollinger_mavg()

        return datos_trabajo