import os
import json
import pandas as pd
import yfinance as yf
from pathlib import Path

def currentStorage(fecha_fin, dia_posterior, ticker, nombre):
    rutaSalida = Path(__file__).parent.parent.parent / "test" / "products"
    print(f"[Almacenamiento reciente] [{nombre}] Descargando datos de {nombre} ({ticker}) ...")
    df = yf.download(ticker, start=fecha_fin.isoformat(), end=dia_posterior.isoformat(),auto_adjust=False)
    df = df.reset_index()

    # Si las columnas son MultiIndex, usa solo el primer nivel
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    if not df.empty:
        subcarpeta = os.path.join(rutaSalida, nombre)
        os.makedirs(subcarpeta, exist_ok=True)
        ruta_archivo = os.path.join(
            subcarpeta,
            f"{nombre}_{ticker.replace('.MX', '')}_historico_mongo.json"
        )

        # Leer el archivo NDJSON si existe
        if os.path.exists(ruta_archivo):
            documentos_previos = []
            with open(ruta_archivo, 'r', encoding='utf-8') as f:
                for line in f:
                    documentos_previos.append(json.loads(line))
            df_hist = pd.DataFrame(documentos_previos)
            df_hist['Date'] = pd.to_datetime(df_hist['Date'].apply(lambda d: d['$date']), utc=True)
            # Concatenar y eliminar duplicados
            df_total = pd.concat([df_hist, df], ignore_index=True)
            if 'Date' in df_total.columns:
                df_total = df_total.drop_duplicates(subset=['Date'])
        else:
            df_total = df

        # Guardar en NDJSON con formato MongoDB BSON extendido en Date
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
                    "Volume": int(row['Volume']) if pd.notna(row['Volume']) else None
                }
                f.write(json.dumps(document, ensure_ascii=False) + "\n")

        print(f"[Almacenamiento reciente] [{nombre}] ✅ Guardado en: {ruta_archivo}")
        print(f"[MongoDB] Total documentos: {len(df_total)}")

    else:
        print(f"[Almacenamiento reciente] [{nombre}] ⚠️ No se encontraron datos para {ticker}")

