import pandas as pd
from domain.dataset import Dataset

class Datasetexcel(Dataset):
    def __init__(self, fuente):
        super().__init__(fuente)
    
    def cargar_datos(self):
        try:
            df = pd.read_excel(self.fuente)
            self.datos = df
            print("El excel se ha cargado correctamente")
            self.validar_datos()
        except Exception as e:
            print(f"Error al cargar el archivo .csv: {e}")