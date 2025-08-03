import pandas as pd
import os
from typing import Optional
import json
from pathlib import Path

class saveTransformations:
    def __init__(self,datos:pd.DataFrame,ticker:str,nombre:str):
        self._data_original = datos
        self.ticker = ticker
        self.rutaSalida = Path(__file__).parent.parent.parent / "test" / "tecnicalAnalysis"
        self.nombre = nombre
        
    def storageTecnicalAnalysis(self, data: Optional[pd.DataFrame] = None):

        datos_trabajo = data.copy() if data is not None else self._data_original.copy()
        
        if not datos_trabajo.empty:
            datos_trabajo['ticker'] = self.ticker
            # Usar solo pathlib.Path para consistencia
            subcarpeta = self.rutaSalida / self.nombre
            subcarpeta.mkdir(parents=True, exist_ok=True)
            ruta_archivo = subcarpeta / f"{self.nombre}_{self.ticker.replace('.MX', '')}_tecnical_analysis.json"

            df_total = datos_trabajo

            # Crear lista documentos para guardar como NDJSON

            with open(ruta_archivo, 'w', encoding='utf-8') as f:
                for _, row in df_total.iterrows():
                    date_obj = pd.to_datetime(row['Date'], utc=True)

                    document = {
                        "Date": {"$date": date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')},
                        "ticker": row['ticker'],
                        "Open": float(row['Open']) if pd.notna(row['Open']) else None,
                        "High": float(row['High']) if pd.notna(row['High']) else None,
                        "Low": float(row['Low']) if pd.notna(row['Low']) else None,
                        "Close": float(row['Close']) if pd.notna(row['Close']) else None,
                        "Adj Close": float(row['Adj Close']) if pd.notna(row['Adj Close']) else None,
                        "Volume": int(row['Volume']) if pd.notna(row['Volume']) else None,
                        "rendimiento_aritmetico" : float(row["rendimiento_aritmetico"]) if pd.notna(row["rendimiento_aritmetico"]) else None,
                        "rendimiento_logaritmico" : float(row["rendimiento_logaritmico"]) if pd.notna(row["rendimiento_logaritmico"]) else None,
                        "rendimiento_acumulado_arit" : float(row["rendimiento_acumulado_arit"]) if pd.notna(row["rendimiento_acumulado_arit"]) else None,
                        "rendimiento_acumulado_log" : float(row["rendimiento_acumulado_log"]) if pd.notna(row["rendimiento_acumulado_log"]) else None,
                        "volatilidad_005d" : float(row["volatilidad_005d"]) if pd.notna(row["volatilidad_005d"]) else None,
                        "volatilidad_010d" : float(row["volatilidad_010d"]) if pd.notna(row["volatilidad_010d"]) else None,
                        "volatilidad_015d" : float(row["volatilidad_015d"]) if pd.notna(row["volatilidad_015d"]) else None,
                        "volatilidad_020d" : float(row["volatilidad_020d"]) if pd.notna(row["volatilidad_020d"]) else None,
                        "volatilidad_030d" : float(row["volatilidad_030d"]) if pd.notna(row["volatilidad_030d"]) else None,
                        "volatilidad_060d" : float(row["volatilidad_060d"]) if pd.notna(row["volatilidad_060d"]) else None,
                        "volatilidad_090d" : float(row["volatilidad_090d"]) if pd.notna(row["volatilidad_090d"]) else None,
                        "volatilidad_252d" : float(row["volatilidad_252d"]) if pd.notna(row["volatilidad_252d"]) else None,
                        "MA005" : float(row["MA005"]) if pd.notna(row["MA005"]) else None,
                        "MA010" : float(row["MA010"]) if pd.notna(row["MA010"]) else None,
                        "MA012" : float(row["MA012"]) if pd.notna(row["MA012"]) else None,
                        "MA020" : float(row["MA020"]) if pd.notna(row["MA020"]) else None,
                        "MA050" : float(row["MA050"]) if pd.notna(row["MA050"]) else None,
                        "MA060" : float(row["MA060"]) if pd.notna(row["MA060"]) else None,
                        "MA100" : float(row["MA100"]) if pd.notna(row["MA100"]) else None,
                        "MA200" : float(row["MA200"]) if pd.notna(row["MA200"]) else None,
                        "EMA005" : float(row["EMA005"]) if pd.notna(row["EMA005"]) else None,
                        "EMA010" : float(row["EMA010"]) if pd.notna(row["EMA010"]) else None,
                        "EMA012" : float(row["EMA012"]) if pd.notna(row["EMA012"]) else None,
                        "EMA020" : float(row["EMA020"]) if pd.notna(row["EMA020"]) else None,
                        "EMA026" : float(row["EMA026"]) if pd.notna(row["EMA026"]) else None,
                        "EMA050" : float(row["EMA050"]) if pd.notna(row["EMA050"]) else None,
                        "EMA060" : float(row["EMA060"]) if pd.notna(row["EMA060"]) else None,
                        "EMA100" : float(row["EMA100"]) if pd.notna(row["EMA100"]) else None,
                        "EMA200" : float(row["EMA200"]) if pd.notna(row["EMA200"]) else None,
                        "MACD" : float(row["MACD"]) if pd.notna(row["MACD"]) else None,
                        "Signal_MACD" : float(row["Signal_MACD"]) if pd.notna(row["Signal_MACD"]) else None,
                        "ADX" : float(row["ADX"]) if pd.notna(row["ADX"]) else None,
                        "ADX_DI_plus" : float(row["ADX_DI_plus"]) if pd.notna(row["ADX_DI_plus"]) else None,
                        "ADX_DI_less" : float(row["ADX_DI_less"]) if pd.notna(row["ADX_DI_less"]) else None,
                        "RSI" : float(row["RSI"]) if pd.notna(row["RSI"]) else None,
                        "stoch_k" : float(row["stoch_k"]) if pd.notna(row["stoch_k"]) else None,
                        "stoch_d" : float(row["stoch_d"]) if pd.notna(row["stoch_d"]) else None,
                        "stoch_diff" : float(row["stoch_diff"]) if pd.notna(row["stoch_diff"]) else None,
                        "cci" : float(row["cci"]) if pd.notna(row["cci"]) else None,
                        "bb_upper" : float(row["bb_upper"]) if pd.notna(row["bb_upper"]) else None,
                        "bb_lower" : float(row["bb_lower"]) if pd.notna(row["bb_lower"]) else None,
                        "bb_middle" : float(row["bb_middle"]) if pd.notna(row["bb_middle"]) else None
                    }
                    f.write(json.dumps(document, ensure_ascii=False) + "\n")

            print(f"[Almacenamiento Análisis Técnico] [{self.nombre}] ✅ Guardado como NDJSON compatible MongoDB en: {self.rutaSalida}")
            print(f"[Almacenamiento Análisis Técnico] Total documentos: {len(df_total)}")

        else:
            print(f"[Almacenamiento Análisis Técnico] [{self.nombre}] ⚠️ No se encontraron datos para {self.ticker}")
        