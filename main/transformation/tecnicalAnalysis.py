from pandas import DataFrame
import numpy as np
import ta

def indctr_01_roi(datos:DataFrame):
    # Rendimiento aritmético (simple)
    datos['rendimiento_aritmetico'] = datos['Close'].pct_change()

    # Rendimiento logarítmico
    datos['rendimiento_logaritmico'] = np.log(datos['Close'] / datos['Close'].shift(1))

    # Rendimiento acumulado aritmético
    datos['rendimiento_acumulado_arit'] = (1 + datos['rendimiento_aritmetico']).cumprod() - 1

    # Rendimiento acumulado logarítmico
    datos['rendimiento_acumulado_log'] = datos['rendimiento_logaritmico'].cumsum()

    return datos

def indctr_02_volatility(datos:DataFrame):

    # Volatilidad móvil (desviación estándar de 005 días)
    datos['volatilidad_005d'] = datos['rendimiento_logaritmico'].rolling(window=5).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 010 días)
    datos['volatilidad_010d'] = datos['rendimiento_logaritmico'].rolling(window=10).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 015 días)
    datos['volatilidad_015d'] = datos['rendimiento_logaritmico'].rolling(window=15).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 020 días)
    datos['volatilidad_020d'] = datos['rendimiento_logaritmico'].rolling(window=20).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 030 días)
    datos['volatilidad_030d'] = datos['rendimiento_logaritmico'].rolling(window=30).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 060 días)
    datos['volatilidad_060d'] = datos['rendimiento_logaritmico'].rolling(window=60).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 090 días)
    datos['volatilidad_090d'] = datos['rendimiento_logaritmico'].rolling(window=90).std() * np.sqrt(252)  # Anualizada

    # Volatilidad móvil (desviación estándar de 252 días)
    datos['volatilidad_252d'] = datos['rendimiento_logaritmico'].rolling(window=252).std() * np.sqrt(252)  # Anualizada

    return datos

def indctr_03_moving_average_exp(datos:DataFrame):

    # Medias móviles de 5 días de corto plazo
    datos['MA005'] = datos['Close'].rolling(window=5).mean()
    # Medias móviles de 10 días de corto plazo
    datos['MA010'] = datos['Close'].rolling(window=10).mean()
    # Medias móviles de 12 días de corto plazo
    datos['MA012'] = datos['Close'].rolling(window=12).mean()
    # Medias móviles de 20 días de mediano plazo
    datos['MA020'] = datos['Close'].rolling(window=20).mean()
    # Medias móviles de 50 días de mediano plazo
    datos['MA050'] = datos['Close'].rolling(window=50).mean()
    # Medias móviles de 60 días de mediano plazo
    datos['MA060'] = datos['Close'].rolling(window=60).mean()
    # Medias móviles de 100 días de largo plazo
    datos['MA100'] = datos['Close'].rolling(window=100).mean()
    # Medias móviles de 200 días de largo plazo
    datos['MA200'] = datos['Close'].rolling(window=200).mean()

    return datos

def indctr_04_moving_average_ar(datos:DataFrame):

    # Medias móviles exponenciales de 5 días de corto plazo
    datos['EMA005'] = datos['Close'].ewm(span=5, adjust=False).mean()
    # Medias móviles exponenciales de 10 días de corto plazo
    datos['EMA010'] = datos['Close'].ewm(span=10, adjust=False).mean()
    # Medias móviles exponenciales de 12 días de corto plazo
    datos['EMA012'] = datos['Close'].ewm(span=12, adjust=False).mean()
    # Medias móviles exponenciales de 20 días de mediano plazo
    datos['EMA020'] = datos['Close'].ewm(span=20, adjust=False).mean()
    # Medias móviles exponenciales de 26 días de mediano plazo
    datos['EMA026'] = datos['Close'].ewm(span=26, adjust=False).mean()
    # Medias móviles exponenciales de 50 días de mediano plazo
    datos['EMA050'] = datos['Close'].ewm(span=50, adjust=False).mean()
    # Medias móviles exponenciales de 60 días de mediano plazo
    datos['EMA060'] = datos['Close'].ewm(span=60, adjust=False).mean()
    # Medias móviles exponenciales de 100 días de largo plazo
    datos['EMA100'] = datos['Close'].ewm(span=100, adjust=False).mean()
    # Medias móviles exponenciales de 200 días de largo plazo
    datos['EMA200'] = datos['Close'].ewm(span=200, adjust=False).mean()

    return datos

def indctr_05_trend_indicatos(datos:DataFrame):
    # Medias móviles exponenciales de 26 días de mediano plazo
    datos['EMA026'] = datos['Close'].ewm(span=26, adjust=False).mean()

    # Medias móviles exponenciales de 12 días de corto plazo
    datos['EMA012'] = datos['Close'].ewm(span=12, adjust=False).mean()

    datos['MACD'] = datos['EMA012'] - datos['EMA026']
    datos['Signal_MACD'] = datos['MACD'].ewm(span=9, adjust=False).mean()

    # Calcular ADX, +DI y -DI
    datos['ADX'] = ta.trend.adx(datos['High'], datos['Low'], datos['Close'], window=14)
    datos['ADX_DI_plus'] = ta.trend.adx_pos(datos['High'], datos['Low'], datos['Close'], window=14)
    datos['ADX_DI_less'] = ta.trend.adx_neg(datos['High'], datos['Low'], datos['Close'], window=14)

    # Calcular el RSI (por ejemplo, de 14 días)
    datos['RSI'] = ta.momentum.rsi(datos['Close'], window=14)

    # Calcular %K y %D
    datos['stoch_k'] = ta.momentum.stoch(datos['High'], datos['Low'], datos['Close'], window=14, smooth_window=3)
    datos['stoch_d'] = ta.momentum.stoch_signal(datos['High'], datos['Low'], datos['Close'], window=14, smooth_window=3)

    # Calcular la diferencia
    datos['stoch_diff'] = datos['stoch_k'] - datos['stoch_d']

    # Calcular el CCI
    datos['cci'] = ta.trend.cci(high=datos['High'], low=datos['Low'], close=datos['Close'], window=20)

    # Calcular Bandas de Bollinger (20-periodos, 2 desviaciones)
    bb = ta.volatility.BollingerBands(close=datos['Close'], window=20, window_dev=2)
    datos['bb_upper'] = bb.bollinger_hband()
    datos['bb_lower'] = bb.bollinger_lband()
    datos['bb_middle'] = bb.bollinger_mavg()

    return datos