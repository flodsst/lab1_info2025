from abc import ABC, abstractmethod
import pandas as pd


class Dataset(ABC):
    def __init__(self, fuente):
        self.__fuente = fuente
        self.__datos = None

    @property
    def datos(self):
        return self.__datos
    
    @datos.setter
    def datos(self, value):
        #validaciones
        self.__datos = value

    @property
    def fuente(self):
        return self.__fuente

    @abstractmethod
    def cargar_datos(self):
        pass

    def validar_datos(self):
        if self.datos is None:
            raise ValueError("No hay datos")
        
        # Validar campos obligatorios (no nulos)
        if self.datos.isnull().sum().sum() > 0:
            print("Existen campos con datos faltantes")
        
        # Validación de columnas numéricas
        columnas_numericas = self.datos.select_dtypes(include=['number']).columns
        for col in columnas_numericas:
            if not (self.datos[col].apply(lambda x: isinstance(x, (int, float)) or pd.isnull(x))).all():
                print(f"La columna '{col}' contiene valores no numéricos")
        
        # Validación de columnas de tipo string
        columnas_string = self.datos.select_dtypes(include=['object', 'string']).columns
        for col in columnas_string:
            if not (self.datos[col].apply(lambda x: isinstance(x, str) or pd.isnull(x))).all():
                print(f"La columna '{col}' contiene valores no string")
        
        # Validación de columnas de tipo fecha
        columnas_fecha = self.datos.select_dtypes(include=['datetime', 'datetimetz']).columns
        for col in columnas_fecha:
            if not (self.datos[col].apply(lambda x: pd.isnull(x) or pd.api.types.is_datetime64_any_dtype(type(x)))).all():
                print(f"La columna '{col}' contiene valores no fecha")
        
        num_duplicados = self.datos.duplicated().sum()
        if num_duplicados > 0:
            print(f"Se encontraron {num_duplicados} filas duplicadas:")
            print(self.datos[self.datos.duplicated()])
        
        return True

    def transformar_datos(self, columnas_obligatorias=None):
        if self.datos is not None:
            datos_temp = self.datos.copy()
            datos_temp.columns = datos_temp.columns.str.lower().str.replace(" ", "_")
            # Eliminar duplicados
            datos_temp = datos_temp.drop_duplicates()
            # Eliminar filas con nulos en columnas obligatorias
            if columnas_obligatorias:
                datos_temp = datos_temp.dropna(subset=columnas_obligatorias)
            # Reemplazar valores nulos según tipo de dato en columnas no obligatorias
            columnas_no_obligatorias = [col for col in datos_temp.columns if not (columnas_obligatorias and col in columnas_obligatorias)]
            for col in datos_temp.select_dtypes(include=["object", "string"]).columns:
                if col in columnas_no_obligatorias:
                    datos_temp[col] = datos_temp[col].fillna("desconocido").astype(str).str.strip()
            # No reemplazar nulos en columnas numéricas para no afectar cálculos estadísticos
            for col in datos_temp.select_dtypes(include=["datetime", "datetimetz"]).columns:
                if col in columnas_no_obligatorias:
                    datos_temp[col] = datos_temp[col].fillna("-")
            self.datos = datos_temp
            print("Transformaciones aplicadas")
        else:
            print("No hay datos para transformar")
        

    def mostrar_resumen(self):
        return print(self.datos.describe(include='all') if self.datos is not None else "No hay datos")

# Integración automática de la transformación al flujo de trabajo
# Por ejemplo, al cargar los datos:
# dataset = DatasetCSV('ruta/del/archivo.csv')
# dataset.cargar_datos()
# dataset.transformar_datos(columnas_obligatorias=["nombre", "id"])