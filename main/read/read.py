from dateutil.relativedelta import relativedelta
import pandas as pd
import json

def readJson(ruta: str) -> pd.DataFrame:
    documentos = []
    with open(ruta, 'r', encoding='utf-8') as f:
        for linea in f:
            if linea.strip():  # evitar líneas vacías
                doc = json.loads(linea)
                # Extraer y convertir la fecha
                fecha = doc.get("Date", {}).get("$date")
                doc["Date"] = pd.to_datetime(fecha, utc=True) if fecha else None
                documentos.append(doc)

    df = pd.DataFrame(documentos)
    return df