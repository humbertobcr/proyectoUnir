import yfinance as yf
import os
import pandas as pd
import json
from datetime import datetime

import os
import json
from datetime import datetime
import pandas as pd
import yfinance as yf

def historicalStorage(fecha_inicio, fecha_fin, ticker, nombre):
    rutaSalida = "C:/Users/chane/Documents/repositories/proyectoUnir/test/products/"
    print(f"[Almacenamiento histórico] [{nombre}] Descargando datos de {nombre} ({ticker}) ...")
    df = yf.download(ticker, start=fecha_inicio.isoformat(), end=fecha_fin.isoformat())
    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    if not df.empty:
        df['ticker'] = ticker
        subcarpeta = os.path.join(rutaSalida, nombre)
        os.makedirs(subcarpeta, exist_ok=True)
        ruta_archivo = os.path.join(
            subcarpeta,
            f"{nombre}_{ticker.replace('.MX', '')}_historico_mongo.json"
        )
        # Si ya existe el histórico, cargarlo y concatenar
        if os.path.exists(ruta_archivo):
            # Como el archivo es NDJSON, leemos línea a línea y reconstruimos DataFrame
            documentos_previos = []
            with open(ruta_archivo, 'r', encoding='utf-8') as f:
                for line in f:
                    documentos_previos.append(json.loads(line))
            df_hist = pd.DataFrame(documentos_previos)
            # Convertir Date a datetime para concat
            df_hist['Date'] = pd.to_datetime(df_hist['Date'].apply(lambda d: d['$date']))
            df_total = pd.concat([df_hist, df], ignore_index=True)
            if 'Date' in df_total.columns:
                df_total = df_total.drop_duplicates(subset=['Date'])
        else:
            df_total = df

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
                    "Volume": int(row['Volume']) if pd.notna(row['Volume']) else None
                }
                f.write(json.dumps(document, ensure_ascii=False) + "\n")

        print(f"[Almacenamiento histórico] [{nombre}] ✅ Guardado como NDJSON compatible MongoDB en: {ruta_archivo}")
        print(f"[MongoDB] Total documentos: {len(df_total)}")

    else:
        print(f"[Almacenamiento histórico] [{nombre}] ⚠️ No se encontraron datos para {ticker}")


