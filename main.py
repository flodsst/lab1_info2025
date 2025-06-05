from os import path
from domain.dataset_csv import Datasetcsv
from domain.dataset_excel import Datasetexcel
from data.data_saver import DataSaver


#Rutas de archivos --> Fuente

csv_path = path.join(path.dirname(__file__), "files/star_wars_character_dataset.csv")
excel_path = path.join(path.dirname(__file__), "files/star_wars_vehicles_dataset.xlsx")


#Carga y transformación
csv = Datasetcsv(csv_path)
csv.cargar_datos()
csv.transformar_datos(columnas_obligatorias=["name"])
# csv.mostrar_resumen()

excel = Datasetexcel(excel_path)
excel.cargar_datos()
excel.transformar_datos(columnas_obligatorias=["id", "name", "model"])

#Guardar en bbdd
db = DataSaver()
db.guardar_dataframe(csv.datos, 'star_wars_character_dataset')
db.guardar_dataframe(excel.datos, 'star_wars_vehicles_dataset')
