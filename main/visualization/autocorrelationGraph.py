import matplotlib.pyplot as plt
from typing import Optional
import pandas as pd
from pandas import DataFrame
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import numpy as np

class autocorrelationGraphs:
    def __init__(self, ticker:str ,datos:DataFrame):
        self.data_original = datos
        self.ticker = ticker


    def pacfAcf(self, datosf: Optional[DataFrame] = None,Lags:int = 40):
        datos_trabajo = datosf.copy() if datosf is not None else self.data_original.copy()

        plot_acf(datos_trabajo, lags=Lags)
        plot_pacf(datos_trabajo, lags=Lags)

        plt.show()

    def showSerie(self, datosf: Optional[DataFrame] = None):
        datos_trabajo = datosf.copy() if datosf is not None else self.data_original.copy()

        x_ax = [i for i in range(0,len(datos_trabajo))]
        plt.plot(x_ax, datos_trabajo, marker='o')  # marker='o' para puntos en cada valor
        plt.title('Gráfico de líneas sencillo')
        plt.xlabel('Eje X')
        plt.ylabel('Eje Y')
        plt.grid(True)

        plt.show()

    def showCandels(self,datosf: Optional[DataFrame] = None,dias_mostrar:str = 252):
        datos_trabajo = datosf.copy() if datosf is not None else self.data_original.copy()

        if dias_mostrar and len(datos_trabajo) > dias_mostrar:
            data_grafico = datos_trabajo.tail(dias_mostrar).copy()
            print(f"Mostrando últimos {dias_mostrar} días")
        else:
            data_grafico = datos_trabajo.copy()

        # Crear figura con subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10),
                                       gridspec_kw={'height_ratios': [3, 1]})

        # === GRÁFICO PRINCIPAL: VELAS ===

        # Convertir fechas a formato numérico para matplotlib
        fechas_num = mdates.date2num(data_grafico['Date'])

        # Colores para las velas
        color_alcista = '#00FF00'  # Verde para velas alcistas
        color_bajista = '#FF0000'  # Rojo para velas bajistas
        color_borde = '#000000'  # Negro para bordes

        for i, (idx, row) in enumerate(data_grafico.iterrows()):
            fecha_num = fechas_num[i]
            apertura = row['Open']
            maximo = row['High']
            minimo = row['Low']
            cierre = row['Close']

            # Determinar si la vela es alcista o bajista
            es_alcista = cierre >= apertura
            color_vela = color_alcista if es_alcista else color_bajista

            # Calcular dimensiones del cuerpo de la vela
            altura_cuerpo = abs(cierre - apertura)
            y_cuerpo = min(apertura, cierre)

            # 1. Dibujar la línea vertical (sombra/mecha)
            ax1.plot([fecha_num, fecha_num], [minimo, maximo],
                     color=color_borde, linewidth=1.5, solid_capstyle='round')

            # 2. Dibujar el cuerpo de la vela
            ancho_vela = 0.6  # Ancho del cuerpo de la vela

            if altura_cuerpo > 0:  # Solo dibujar si hay cuerpo
                rectangulo = Rectangle(
                    (fecha_num - ancho_vela / 2, y_cuerpo),  # (x, y) esquina inferior izquierda
                    ancho_vela,  # ancho
                    altura_cuerpo,  # alto
                    facecolor=color_vela,
                    edgecolor=color_borde,
                    linewidth=0.8,
                    alpha=0.8
                )
                ax1.add_patch(rectangulo)
            else:
                # Línea horizontal para cuando apertura = cierre (Doji)
                ax1.plot([fecha_num - ancho_vela / 2, fecha_num + ancho_vela / 2],
                         [apertura, apertura], color=color_borde, linewidth=2)

        # === AGREGAR MEDIAS MÓVILES ===

            # Calcular medias móviles
        #data_grafico['MA20'] = data_grafico['EMA020']
        #data_grafico['MA50'] = data_grafico['EMA050']

        # Dibujar medias móviles
        ax1.plot(fechas_num, data_grafico['EMA020'],
                 label='EMA 020', color='blue', linewidth=2, alpha=0.8)
        ax1.plot(fechas_num, data_grafico['EMA050'],
                 label='EMA 050', color='orange', linewidth=2, alpha=0.8)

        # === CONFIGURAR GRÁFICO PRINCIPAL ===
        ax1.set_title(f'Gráfico de Velas Japonesas - {self.ticker}',
                      fontsize=16, fontweight='bold', pad=20)
        ax1.set_ylabel('Precio ($)', fontsize=12)
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)

        # Formatear eje X
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))

        # === GRÁFICO DE VOLUMEN ===

        # Colores para barras de volumen (mismo criterio que velas)
        colores_volumen = [color_alcista if close >= open else color_bajista
                           for close, open in zip(data_grafico['Close'], data_grafico['Open'])]

        ax2.bar(fechas_num, data_grafico['Volume'],
                color=colores_volumen, alpha=0.7, width=0.6)

        ax2.set_ylabel('Volumen', fontsize=12)
        ax2.set_xlabel('Fecha', fontsize=12)
        ax2.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)

        # Formatear eje X del volumen
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))

        # Rotar fechas en ambos gráficos
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # === AGREGAR INFORMACIÓN ADICIONAL ===

        # Información en el título
        precio_actual = data_grafico['Close'].iloc[-1]
        precio_anterior = data_grafico['Close'].iloc[0]
        cambio_porcentual = ((precio_actual / precio_anterior) - 1) * 100

        info_texto = f'Precio actual: ${precio_actual:.2f} | Cambio período: {cambio_porcentual:+.2f}%'
        fig.suptitle(info_texto, fontsize=12, y=0.02)

        # Ajustar layout
        plt.tight_layout()
        plt.subplots_adjust(bottom=0.1)

        # Mostrar gráfico
        plt.show()

        return fig
