"""
     Almacenamiento, transformación y simulación de las acciones

     Principal: Ejecución de las fases de los datos
"""
# Librerías internas
from asignment.schedVariables import obtener_fecha_consulta
from storage.historicalStorage import historicalStorage
from storage.currentStorage import currentStorage

from read.read import readJson

# Librerías externas
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Nombre de las empresas a descargar sus precios de acciones
empresas_bmv = {
    "AMX": "AmericaMovil",
    "WALMEX.MX": "WalmartMexico",
    "GFNORTEO.MX": "Banorte",
    "BIMBOA.MX": "Bimbo",
    "TLEVISACPO.MX": "Televisa",
    "ALSEA.MX": "Alsea",
    "CEMEXCPO.MX": "Cemex",
    "FEMSAUBD.MX": "Femsa",
    "PE&OLES.MX": "Peñoles",
    "KIMBERA.MX": "KimberlyClark"
}


"""
# Cálculo de los últimos días hábiles

ultimo_dia_habil = obtener_fecha_consulta()
dia_posterior = ultimo_dia_habil + pd.Timedelta(days=2)
fecha_inicio = ultimo_dia_habil - relativedelta(years=2)
fecha_fin = ultimo_dia_habil
#fecha_fin = ultimo_dia_habil - pd.Timedelta(days=1)
ultimo_ejercicio = ultimo_dia_habil - pd.Timedelta(days=1)

print(f"Último día hábil: {ultimo_dia_habil}")
print(f"Fecha Inicio del análisis: {fecha_inicio}")
print(f"Fecha Fin del análisis: {fecha_fin}")
print(f"Último ejercicio : {ultimo_ejercicio}")
print(f"Día posterior : {dia_posterior}")

# Almacenamiento histórico de las acciones

for ticker, nombre in empresas_bmv.items():
    historicalStorage(fecha_inicio, fecha_fin, ticker, nombre)
    print(f"\n[Almacenamiento histórico] [{nombre}] Completo ✅")

print(f"\n[Almacenamiento histórico] Completo ✅. Desde {fecha_inicio} hasta{fecha_fin}")



ruta  = "C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Alsea/Alsea_ALSEA_historico_mongo.json"

# Leer cada línea como un diccionario
documentos = []
with open(ruta, 'r', encoding='utf-8') as f:
    for linea in f:
        if linea.strip():  # evitar líneas vacías
            doc = json.loads(linea)
            # Extraer y convertir la fecha
            fecha = doc.get("Date", {}).get("$date")
            doc["Date"] = pd.to_datetime(fecha, utc=True) if fecha else None
            documentos.append(doc)

# Crear DataFrame
df = pd.DataFrame(documentos)

# Mostrar primeros valores
print(df.head())


# Almacenamiento del ejecicio más reciente

for ticker, nombre in empresas_bmv.items():
    currentStorage(fecha_fin,dia_posterior, ticker, nombre)
    print(f"\n[Almacenamiento reciente] [{nombre}] Completo ✅")

print(f"\n[Almacenamiento reciente] Completo ✅. De {dia_posterior} ")


ruta  = "C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Alsea/Alsea_ALSEA_historico_mongo.json"

# Leer cada línea como un diccionario
documentos = []
with open(ruta, 'r', encoding='utf-8') as f:
    for linea in f:
        if linea.strip():  # evitar líneas vacías
            doc = json.loads(linea)
            # Extraer y convertir la fecha
            fecha = doc.get("Date", {}).get("$date")
            doc["Date"] = pd.to_datetime(fecha, utc=True) if fecha else None
            documentos.append(doc)

# Crear DataFrame
df = pd.DataFrame(documentos)

# Mostrar primeros valores
print(df.head())
"""

ListaRutas = [
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Alsea/Alsea_ALSEA_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/AmericaMovil/AmericaMovil_AMX_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Banorte/Banorte_GFNORTEO_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Bimbo/Bimbo_BIMBOA_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Cemex/Cemex_CEMEXCPO_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Femsa/Femsa_FEMSAUBD_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/KimberlyClark/KimberlyClark_KIMBERA_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Peñoles/Peñoles_PE&OLES_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Televisa/Televisa_TLEVISACPO_historico_mongo.json",
"C:/Users/chane/Documents/repositories/proyectoUnir/test/products/WalmartMexico/WalmartMexico_WALMEX_historico_mongo.json"
]


ruta  = "C:/Users/chane/Documents/repositories/proyectoUnir/test/products/Alsea/Alsea_ALSEA_historico_mongo.json"

# Leer cada línea como un diccionario

df = readJson(ruta)

# Mostrar primeros valores

# Cálculo de las variables de rendimientos
from transformation.tecnicalAnalysis import tecnicalAnalysis

analisisTecnico = tecnicalAnalysis(df)

roiDf = analisisTecnico.indctr_01_roi()

# Cálculo de las variables de volatilidad
varDf = analisisTecnico.indctr_02_volatility(roiDf)

# Cálculo de las variables de medias móviles exponenciales
movingAverageExpDF = analisisTecnico.indctr_03_moving_average_exp(varDf)

# Cálculo de las variables de medias móviles aritméticas
movingAverageArDF = analisisTecnico.indctr_04_moving_average_ar(movingAverageExpDF)

# Cálculo de las variables de tendencia
trendIndicatosDF = analisisTecnico.indctr_05_trend_indicatos(movingAverageArDF)

print(trendIndicatosDF.columns)

