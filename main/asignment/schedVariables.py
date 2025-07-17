from datetime import datetime, timedelta
import pandas as pd

def obtener_ultimo_dia_habil(fecha):
    # Retrocede hasta encontrar un día hábil (lunes a viernes)
    while fecha.weekday() >= 5:  # 5 = sábado, 6 = domingo
        fecha -= timedelta(days=1)
    return fecha

def obtener_fecha_consulta():
    ahora = datetime.now()
    hoy = ahora.date()
    hora_actual = ahora.time()
    # Si es sábado o domingo, buscar el último viernes
    if hoy.weekday() >= 5:
        return obtener_ultimo_dia_habil(hoy)
    # Si es lunes antes de las 16:00, buscar el viernes anterior
    if hoy.weekday() == 0 and hora_actual < datetime.strptime("16:00", "%H:%M").time():
        return obtener_ultimo_dia_habil(hoy - timedelta(days=1))
    # Si es cualquier día antes de las 16:00, usar el día anterior (hábil)
    if hora_actual < datetime.strptime("16:00", "%H:%M").time():
        return obtener_ultimo_dia_habil(hoy - timedelta(days=1))
    # Si es después de las 16:00, usar hoy
    return hoy