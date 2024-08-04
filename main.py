"""
main.py:
    ETAPA 1: Ingesta
            Se consulta a API distintos parametros meteorologico 
            por distintas localidades cercanas 
    ETAPA 2: Almacenamiento
            Se almacena estos datos en dataframes utilizando el metodo
            incremental en formato Parquet.
    ETAPA 3: Carga en BD
            Se crea una base de datos PostGres en Aiven para cargar
            los frames en formato Parquet.

"""
__author__ = "Nahuel Vargas"
__maintainer__ = "Nahuel Vargas"
__email__ = "nahuvargas24@gmail.com"
__copyright__ = "Copyright 2024"
__version__ = "0.0.2"

# Librerias
import requests
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from pprintpp import pprint as pp
from configparser import ConfigParser
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from utils_db import *

# Macros para aceder a distintos endpoints
CLIMA_ACTUAL = "current.json"
UBICACION = "search.json"
# Ubicacion de archivos
dir_bronze = "datalake/bronze/weatherapi"
dir_silver = "datalake/silver/weatherapi"
# Listado de localidades cercanas
locations = [
    "Castelar, Buenos Aires",
    "Moron, Buenos Aires",
    "Ituzaingo, Buenos Aires",
    "Haedo, Buenos Aires",
    "El Palomar, Buenos Aires",
    "Villa Tesei, Buenos Aires",
    "Hurlingham, Buenos Aires",
    "San Justo, Buenos Aires",
    "Ramos Mejía, Buenos Aires",
    "Ciudadela, Buenos Aires",
    "Lomas del Mirador, Buenos Aires"
]

# Nombres de tablas
tabla_incremental = "clima_v2_incremental"
tabla_full = "clima_v2_full"

# ----- Funciones para obtener datos de API ------ #
def get_data(base_url, endpoint, params=None, headers=None):
    """
    Consultar a API y recibe una respuesta en .json
    :param base_url: url de API
    :param params: parametros a considerar en la solicitud (ver doc de API)
    :param headers: encabezados para solicitud, no aparecen en URL (usar para keys)
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        return None

def build_table(json_data):
    """
    Estructura a .json en tabla
    :param json_data: texto en formato json
    :return df: dataframe- tabla de pandas
    """
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None

def get_token():
    """
    Accedo a archivo .conf para obtener contraseña
    :return access_token: contraseña user para API
    """
    parser = ConfigParser()
    parser.read("pipeline.conf")
    access_token=parser["api_credentials"]["client_secret"]

    return access_token

def access_API(access_token, endpoint, location):
    """
    Acceso a API con carga de su parametros de solicitud
    :param access_token: contraseña para acceder a API
    :param endpoint: endpoint de la API 
    :return json_data: .json de rta a solicitud a API
    """
    base_url = "http://api.weatherapi.com/v1" # Se actualiza cada 15min
    params ={
        "q": location,
    }
    headers ={
        "key": access_token,
    }
    # Solicitud a API
    json_data = get_data(base_url, endpoint=endpoint, params=params, headers=headers)
    #pp(json_data)

    return json_data

def multi_query(access_token):
    """
    Consulta de varias localidades de algunos parametros a API
    :param access_token: contraseña para acceder a API
    :return data_api_meas: diccionario de varias mediciones
    :return data_api_location: diccionario de atributos de localidades
    """
    # Listas para almacenar diccionarios por cada consulta a API
    data_api_meas = [] 
    data_api_location = []

    for location in locations:
        # Acceso a API - consulto por localidades cercanas
        json_data_meas = access_API(access_token, CLIMA_ACTUAL, location)

        # Combino diccionarios para ver todos los datos dinamicos relevantes
        data_meas = json_data_meas["current"]
        data_meas_aux = json_data_meas["location"]
        data_meas = data_meas | data_meas_aux

        # Armo lista de diccionario de la distintas localidades consultadas
        data_api_meas.append(data_meas)

    return data_api_meas

# ----- Funciones para optimizar registro de datos   ------ #
def save_to_parquet(df, output_path, partition_cols=None):
    """
    Recibe un dataframe, se recomienda que haya sido convertido a un formato tabular,
    y lo guarda en formato parquet.
    :param df: Dataframe a guardar.
    :param output_path: Ruta donde se guardará el archivo. Si no existe, se creará.
    :param partition_cols: Columna/s por las cuales particionar los datos.
    """

    # Crear el directorio si no existe
    directory = os.path.dirname(output_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    df.to_parquet(
        output_path,
        engine="fastparquet",
        partition_cols=partition_cols
        )

def read_data():
    """
    Lectura de archivos parquet en la carpeta broze
    :return df_measure : dataframe con datos de columnas relevantes y encabezados renbombrados
    """
    # Lectura de archivos parquet
    df_measure = pd.read_parquet(
        path=f"{dir_bronze}/measures",
        engine="fastparquet",
        )
    # Selecciono algunas columnas
    df_measure = df_measure[["name","localtime","last_updated", "temp_c", "humidity", "condition.text",]]
    # Renombramos columnas para que coincidan con la tabla de la base de datos
    df_measure.columns = ["localidad", "time_solicitud", "time_update", "temperatura", "humedad", "condicion_climatica"]

    return df_measure

# ----- Funciones para interactuar con base de datos ------ #
def create_table_db(engine, name_table):
    # **** NOTA : Cuidado con las mayusculas y minusculas ****
    # Defino esquema de tabla para base de datos
    query = text(f"""
    -- Creamos una tabla sobre datos 
    CREATE TABLE IF NOT EXISTS public.{name_table} (
        id SERIAL PRIMARY KEY,
        localidad VARCHAR(255),
        time_solicitud TIMESTAMP,
        time_update TIMESTAMP,
        temperatura FLOAT,
        humedad FLOAT,
        condicion_climatica VARCHAR(255)
    );
    """)
    # Ejecutamos la query
    with engine.connect() as conn, conn.begin():
        conn.execute(query)

def load_to_db(engine, df_measure, name_table):
    """
    Carga de dataframe a base de datos
    :param engine: cursor de conexion a base de datos
    :param df_measure: dataframe a cargar
    :param name_table: nombre de tabla a cargar los datos
    """
    # Carga de dataframe en base de datos
    try:
        with engine.connect() as conn, conn.begin():
            df_measure.to_sql(
                f"{name_table}",
                schema="public",
                con=conn,
                if_exists="append", # opcionse: "replace" o "append"
                method="multi",
                index=False
            )
    except UniqueViolation:
        print("error de clave primaria")

# ------  Ejecucion de programa  ------ #
# Obtengo contraseña
access_token = get_token()
# Consulto por varias localidades cercanas
data_api_meas = multi_query(access_token)
# Normalizo en formato de dataframes
df_meas = build_table(data_api_meas)

# Convierto en formato de datetime columnas de fecha y hora
df_meas["last_updated"] = pd.to_datetime(df_meas.last_updated)
df_meas["localtime"] = pd.to_datetime(df_meas.localtime)

# Creo columnas nuevas de fecha y hora por separado
df_meas["last_update_date"] = df_meas.last_updated.dt.date
df_meas["last_update_hour"] = df_meas.last_updated.dt.hour

# ALMACENAMIENTO PARQUET
# Guardo en formato Parquet
save_to_parquet(
    df=df_meas,
    output_path=f"{dir_bronze}/measures/data.parquet",
    partition_cols=["last_update_date", "last_update_hour"]
    )

# Lectura de archivos parquet con datos dinamicos cada 15min a partir de la 00:00:00
df_measure = read_data()

# Filtro para solo cargar los ultimos datos de hace 10min (ejemplo)
correccion_arg = timedelta(hours=3)
start_datetime = datetime.utcnow() - correccion_arg - timedelta(minutes=10)
df_measure = df_measure[df_measure['time_update'] >= start_datetime]

# Fuerzo nulo para probar algunos metodos
df_measure.loc[df_measure['localidad'] == 'Castelar', 'temperatura'] = np.nan
df_measure.loc[df_measure['localidad'] == 'Hurlingham', 'temperatura'] = np.nan

# Dataframe a cargar en base de datos
print("Dataframe a cargar en BD:")
print(df_measure)
print(df_measure.info(memory_usage='deep'))


# Acceso a base de datos postgre
engine = connect_to_db(
    "pipeline.conf",
    "postgres",
    "postgresql+psycopg2"
    )

# CARGA EN BASE DE DATOS FULL
# Creo esquema de tabla  - solo se crea si no existe
create_table_db(engine, f"{tabla_full}")
# Carga de Dataframe a base de datos FULL
load_to_db(engine, df_measure, f"{tabla_full}")

# EXTRACCION FULL
query = text(f"""
SELECT *
FROM public.{tabla_full}
""")

with engine.connect() as conn, conn.begin():
    df_measure_check = pd.read_sql(query, conn)

print("Dataframe leido de la base datos:")
print(df_measure_check)
print(df_measure_check.info(memory_usage='deep'))

# TRANSFORMACION
# Manejo de nulos
print("Total datos nulos en 'temperatura': ", 
    df_measure_check.temperatura.isnull().sum())

# EXTRACCION INCREMENTAL
## Actualizo fecha de ultima actualizacion 
get_metadata_db(engine)
## Obtengo de la base de datos aquellos que sean de una fecha superior
## a la leida en el archivo metadata --- se utiliza el campo 'time_solicitud'
## para comparar las fechas
df_measure_update = extract_incremental_data(
    engine, f'{tabla_full}', 'metadata/metadata_ingestion.json'
    )

print("Dataframe obtenido incremental")
print(df_measure_update)

# CARGA EN BASE DE DATOS INCREMETAL
# Creo tabla para datos incrementales
create_table_db(engine, f"{tabla_full}")
# Leo base de datos para saber que datos tengo
# Comparo datos leidos con los obtenidos de la extraccion incremental -- dataframe
# Si algun campo cambio se actualiza y si no existe se agrega - teniendo en cuenta el campos de referencia (localidad) -- dataframe
# Del nuevo dataframe obtenido se carga a base de datos
# Carga de Dataframe a base de datos INCREMETAL
load_to_db(engine, df_measure_update, f"{tabla_incremental}")

df_measure_fin = df_measure.copy()
# Solo actualizo si hay algun cambio
if not df_measure_update.empty:
    # Asigno indice temporal como guia para actualizar los datos por localidad
    df_measure.set_index('localidad', inplace=True)
    df_measure_update.set_index('localidad', inplace=True)

    for col in df_measure_update.columns:
        df_measure_fin.update(df_measure_update[col])

    # Restablecer el índice
    df_measure_fin = df_measure.reset_index()

print("Dataframe final con datos actualizados:")
print(df_measure_fin)


# OBS.: Falta agregar rta cuando no se obtiene valores de API