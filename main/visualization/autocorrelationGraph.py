import matplotlib.pyplot as plt
from typing import Optional
import pandas as pd
from pandas import DataFrame
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

class autocorrelationGraphs:
    def __init__(self, datos:DataFrame):
        self.data_original = datos


    def pacfAcf(self, datosf: Optional[DataFrame] = None):
        datos_trabajo = datosf.copy() if datosf is not None else self.data_original.copy()

        plot_acf(datos_trabajo, lags=20)
        plot_pacf(datos_trabajo, lags=20)

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
