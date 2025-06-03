import pandas as pd
from domain.dataset import Dataset

class Datasetcsv(Dataset):
    def __init__(self, fuente):
        super().__init__(fuente)
    
    def cargar_datos(self):
        try:
            df = pd.read_csv(self.fuente)
            self.datos = df
            print("El csv se ha cargado correctamente")
            self.validar_datos()
        except Exception as e:
            print(f"Error al cargar el archivo .csv: {e}")
