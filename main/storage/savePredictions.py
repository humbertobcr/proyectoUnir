import pandas as pd
from typing import Optional
import json
from pathlib import Path

class savePredictions:
    def __init__(self, datos: pd.DataFrame, ticker: str, nombre: str):
        self._data_original = datos
        self.ticker = ticker
        self.rutaSalida = Path(__file__).parent.parent.parent / "test" / "predictions"
        self.nombre = nombre

    def storagePredictions(self, data: Optional[pd.DataFrame] = None):

        datos_trabajo = data.copy() if data is not None else self._data_original.copy()

        if not datos_trabajo.empty:
            datos_trabajo['ticker'] = self.ticker
            # Usar solo pathlib.Path para consistencia
            subcarpeta = self.rutaSalida / self.nombre
            subcarpeta.mkdir(parents=True, exist_ok=True)
            ruta_archivo = subcarpeta / f"{self.nombre}_{self.ticker.replace('.MX', '')}_predictions.json"

            df_total = datos_trabajo

            # Crear lista documentos para guardar como NDJSON

            with open(ruta_archivo, 'w', encoding='utf-8') as f:
                for _, row in df_total.iterrows():
                    date_obj = pd.to_datetime(row['Date'], utc=True)

                    document = {
                        "Date": {"$date": date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')},
                        "ticker": row['ticker'],
                        "frcst_sarimax_01_mean": float(row["frcst_sarimax_01_mean"]) if pd.notna(row["frcst_sarimax_01_mean"]) else None,
                        "frcst_sarimax_01_low": float(row["frcst_sarimax_01_low"]) if pd.notna(row["frcst_sarimax_01_low"]) else None,
                        "frcst_sarimax_01_upper": float(row["frcst_sarimax_01_upper"]) if pd.notna(row["frcst_sarimax_01_upper"]) else None
                    }
                    f.write(json.dumps(document, ensure_ascii=False) + "\n")

            print(
                f"[Almacenamiento Pronósticos] [{self.nombre}] ✅ Guardado como NDJSON compatible MongoDB en: {self.rutaSalida}")
            print(f"[Almacenamiento Pronósticos] Total documentos: {len(df_total)}")

        else:
            print(f"[Almacenamiento Pronósticos] [{self.nombre}] ⚠️ No se encontraron datos para {self.ticker}")