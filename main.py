"""
main.py:
    1- Extraccion incremental y full de API, minimo 2 enpoint (estatico y dinamico)
    2- Convertir en datos crudos en DataFrames de Pandas
    3- Guardas datos crudos (o leves transformaciones) en formato Parquet -> como en DataLake
        OBS: Particinar por fecha/hora en dinamico y dato relevante en el estatico
    4- Aplicar minimo 4 tareas de transformación
    5- Guardar en una base de datos OLAP - Postgre de Aiven
"""
__author__ = "Nahuel Vargas"
__maintainer__ = "Nahuel Vargas"
__email__ = "nahuvargas24@gmail.com"
__copyright__ = "Copyright 2024"
__version__ = "0.0.3"

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
ENPOINT_MEAS_NOW = "current.json" #endpoint dinamico
ENPOINT_HISTORY = "history.json" #endpoint estatico

# Ubicacion de archivos
dir_bronze = "datalake/bronze/weatherapi"
dir_silver = "datalake/silver/weatherapi"

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

def access_API(access_token, endpoint, location, dt=None):
    """
    Acceso a API con carga de su parametros de solicitud
    :param access_token: contraseña para acceder a API
    :param endpoint: endpoint de la API 
    :param dt: fecha hora especifica que se solicita
    :return json_data: .json de rta a solicitud a API
    """
    base_url = "http://api.weatherapi.com/v1" # Se actualiza cada 15min
    params ={
        "q": location,
        "aqi": "no",
        "alerts": "no",
        "dt": dt
    }
    headers ={
        "key": access_token,
    }
    # Solicitud a API
    json_data = get_data(base_url, endpoint=endpoint, params=params, headers=headers)

    return json_data

def multi_query(access_token, endpoint, dt=None):
    """
    Consulta de varias localidades de algunos parametros a API
    :param access_token: contraseña para acceder a API
    :param endpoint: endpoint a solicitar
    :param dt: fecha hora especifica que se solicita
    :return data_api: diccionario de varias consultas por diferentes localidades
    """
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

    # Listas para almacenar diccionarios por cada consulta a API
    data_api = [] 

    for location in locations:
        # Acceso a API - consulto por localidades cercanas
        json_data = access_API(access_token, endpoint, location, dt)
        # Armo lista de diccionario de la distintas localidades consultadas
        data_api.append(json_data)

    return data_api

def concat_data_API(df_data_history, start_time, end_time):
    """
    Accedo a diccionario de diccionario para concatenarlo al comienzo
    de la cadena de diccionarios como dataframe, teniendo en cuenta el horario
    solicitado por el limite de solicitud disponible de la API 

    :param df_data_history: diccionario data por la API 
    :param start_time: datetime de inicio
    :param end_time: datatime de fin 
    :return diccionario dado junto a diccionario de horario en uno solo
    """
    # Accedo a diccionario especifico forecast y convierto en dataframe
    df_aux = build_table(build_table(df_data_history['forecast.forecastday'])[0])
    # Accedo a diccionario hour
    df_aux2 = df_aux["hour"]

    # Convertir los límites de tiempo a objetos datetime
    #start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M')
    #end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M')

    # Lista vacia para cargar unicamente horario solicitado a API
    list_data = []

    # Debido a que la API devuelve todos los horarios del dia, es decir, de 00:00 a 23:00, 
    # se emplea logica para solo filtrar aquel horario solicitado
    for index, value in df_aux2.items():
        df_aux3 = build_table(df_aux2[index])
        filtered_times = df_aux3[(pd.to_datetime(df_aux3["time"]) >= start_time) & (pd.to_datetime(df_aux3["time"]) <= end_time)]
        list_data.append(filtered_times)

    # Concateno frames de la lista para crear un unico dataframe
    df_concat = pd.concat(list_data).reset_index(drop=True)
    df_combined = df_data_history.merge(df_concat, how='outer', left_index=True, right_index=True)

    return df_combined

def extract_incremental_data(df_data_history_mod, start_time, end_time):
    """
    Extracción incremental empleando metodo de filtrado debido a
    limitaciones de solicitud a la API
    :param df_data_history_mod: diccionario data por la API 
    :param start_time: datetime de inicio
    :param end_time: datatime de fin 
    :return df_data_history_mod_update: diccionario si hay dataframe actualizado
    """
    # Obtener la columna incremental y su último valor del archivo
    state = read_state_from_json('metadata/metadata_ingestion.json')
    last_value = state["clima_v3"]["last_value"]
    incremental_column = state["clima_v3"]["incremental_column"]

    # Filtro solo aquellos datos que sean mayores al ultimo datatime registrado en archivo state
    df_data_history_mod_update = df_data_history_mod[df_data_history_mod["time"] > f"{last_value}"]

    if not df_data_history_mod_update.empty:
        print("Hay datos para actualizar")

        # Obtengo mayor datetime del dataframe filtrado
        new_value = df_data_history_mod_update["time"].max()
        # Convierto en formato datatime
        new_value = datetime.strptime(new_value, '%Y-%m-%d %H:%M')

        # Actualizo fecha de ultima actualizacion del archivo metadata_ingestion.json
        update_incremental_value(
            state, 'metadata/metadata_ingestion.json', "clima_v3", new_value)

        return df_data_history_mod_update

    else:
        print("No hay datos nuevos para actualizar")
        return pd.DataFrame()

def generated_error_random(df_meas, df_history):
    """
    Genera errores de duplicados y valores nulos de manera aleatoria
    unicamente con el fin de testear manipulacion de datos

    :param df_data_meas:  dataframe de mediciones
    :param df_historyu_mod: dataframe de history
    :return df_data_meas_error:  dataframe de mediciones con errores
    :return df_history_mod_error: dataframe de history con errores
    """

    # Agrego valores nulos random
    # Obtengo numero de columnas y filas en cada dataframe
    num_row_df_meas = len(df_meas)
    num_column_df_meas = len(df_meas.columns)
    num_row_df_history = len(df_history)
    num_column_df_history = len(df_history.columns)

    # Genero 20 valores nulos random
    for x in range(20):
        row_null_df_meas = np.random.randint(0, num_row_df_meas)
        column_null_df_meas = np.random.randint(0, num_column_df_meas)

        row_null_df_history = np.random.randint(0, num_row_df_history)
        column_null_df_history = np.random.randint(0, num_column_df_history)

        df_meas.iat[row_null_df_meas, column_null_df_meas] = np.nan    
        df_history.iat[row_null_df_history, column_null_df_history] = np.nan    

    # Creo duplicados
    df_meas_duplicado_radom = df_meas.sample(frac=0.5, random_state=1)
    df_history_duplicado_radom = df_history.sample(frac=0.3, random_state=1)
    
    # Concateno duplicados
    df_meas_error = pd.concat([df_meas, df_meas_duplicado_radom], ignore_index=True)
    df_history_error = pd.concat([df_history, df_history_duplicado_radom], ignore_index=True)
    
    #print(df_data_meas_error)
    #print(df_data_history_mod_error)

    return df_meas_error, df_history_error

# ------------- Funciones de transformaciones ------------- #
def filtro_columnas(df_data_meas, df_data_history_mod):
    """
    Filtro y reasigno nombre de columnas relevantes 

    :param df_data_meas:  dataframe de mediciones
    :param df_historyu_mod: dataframe de history
    :return df_data_meas:  dataframe de mediciones filtrado
    :return df_history_mod: dataframe de history filtrado
    """

    # Conservo solo datos relevantes y re-nombre columnas
    df_data_meas = df_data_meas[["location.name", "location.region", "location.country", "location.localtime", "location.lat", "location.lon", "current.condition.text"]]
    df_data_meas.columns = ["localidad", "region", "pais", "local_time", "latitud", "longitud", "cond_climatica"]

    df_data_history_mod = df_data_history_mod[["location.name", "update_date", "update_hour", "temp_c", "pressure_mb", "precip_mm", "humidity"]]
    df_data_history_mod.columns = ["localidad", "fecha", "hora", "temp_c", "presion_mb", "precip_mm", "humedad"]

    # Convierto tipo de dato
    df_data_meas['local_time'] = pd.to_datetime(df_data_meas['local_time'])
    df_data_history_mod['fecha'] = pd.to_datetime(df_data_history_mod['fecha'])

    # Asigno formato de fecha y hora
    df_data_meas['local_time'] = df_data_meas['local_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_data_history_mod["fecha"] = df_data_history_mod["fecha"].dt.strftime('%Y-%m-%d')

    #print(df_data_meas_filter)
    #print(df_data_history_mod_filter)

    return df_data_meas, df_data_history_mod

def manejo_nulos(df_data_meas, df_data_history_mod):
    """
    Borro filas en las que su campo localidad sea un valor nulo,
    para el caso de df_meas, considero tambien la region y el pais
    que no sean nulos

    :param df_data_meas:  dataframe de mediciones
    :param df_historyu_mod: dataframe de history
    :return df_data_meas_no_null:  dataframe de mediciones sin nulos e imputados
    :return df_history_mod_no_null: dataframe de history filtrado sin nulos e imputados
    """

    num_null_df_meas = df_data_meas.localidad.isnull().sum()
    num_null_df_history = df_data_history_mod.localidad.isnull().sum()
    
    #print("Nulos en dataframe:")    
    #print("Nulos en localidad df_meas: ", num_null_df_meas)
    #print("Nulos en localidad df_history_mod: ", num_null_df_history)

    if num_null_df_meas:
        df_data_meas_no_null = df_data_meas.dropna(subset=['localidad', 'region', 'pais'])
        # Imputo columnas con valores nulos
        imputation_mapping_meas = {
            "local_time": "1900-01-01 00:00:00",
            "latitud": -1,
            "longitud": -1,
            "cond_climatica": "Nada"
        }
        df_data_meas_no_null = df_data_meas_no_null.fillna(imputation_mapping_meas)
    else:
        df_data_meas_no_null = df_data_meas.copy()

    if num_null_df_history:
        df_data_history_mod_no_null = df_data_history_mod.dropna(subset=['localidad'])

        imputation_mapping_history_mod = {
            "hora": 0,
            "fecha": "1900-01-01",
            "temp_c": -1,
            "presion_mb": -1,
            "precip_mm": -1,
            "humedad": -1
        }

        df_data_history_mod_no_null = df_data_history_mod_no_null.fillna(imputation_mapping_history_mod)
    else:
        df_data_history_mod_no_null = df_data_history_mod.copy()

    #print("luego de imputacion")
    #print(df_data_meas_no_null)
    #print(df_data_history_mod_no_null)

    return df_data_meas_no_null, df_data_history_mod_no_null

def manejo_duplicados(df_data_meas, df_data_history_mod):
    """
    Asigno formato a cada columnas

    :param df_data_meas:  dataframe de mediciones
    :param df_historyu_mod: dataframe de history
    :return df_data_meas_no_dup:  dataframe de mediciones sin duplicados
    :return df_history_mod_no_dup: dataframe de history sin duplicados
    """
    # Ordeno por localidad para observar duplicados
    df_data_meas = df_data_meas.sort_values(by=["localidad", "local_time"])
    df_data_history_mod = df_data_history_mod.sort_values(by=["localidad", "fecha"])

    #print("datos ordenados:")
    #print(df_data_meas)
    #print(df_data_history_mod)

    # Elimino aquellas localidades duplicadas
    df_data_meas_no_dup= df_data_meas.drop_duplicates(subset=['localidad'], keep="last")
    df_data_history_mod_no_dup= df_data_history_mod.drop_duplicates(subset=['localidad'], keep="last")

    #print("sin duplicados:")
    #print(df_data_meas)
    #print(df_data_history_mod)

    return df_data_meas_no_dup, df_data_history_mod_no_dup

def manejo_formats(df_data_meas, df_data_history_mod):
    """
    Asigno formato a cada columnas

    :param df_data_meas:  dataframe de mediciones
    :param df_historyu_mod: dataframe de history
    :return df_data_meas:  dataframe de mediciones con formato
    :return df_history_mod: dataframe de history con formato
    """
    # Conversion de formatos
    conversion_mapping_meas = {
        "localidad": "category",
        "region": "category",
        "pais": "category",
        "local_time": "datetime64[ns]",
        "latitud": "int16",
        "longitud": "int16",
        "cond_climatica": "string"
        }

    conversion_mapping_history_mod = {
        "localidad": "category",
        "fecha": "datetime64[ns]",
        "hora": "int8",
        "temp_c": "float",
        "presion_mb": "float",
        "precip_mm": "float",
        "humedad": "float"
        }

    df_data_meas = df_data_meas.astype(conversion_mapping_meas)
    df_data_history_mod = df_data_history_mod.astype(conversion_mapping_history_mod)

    #print(df_data_meas.info(memory_usage='deep'))
    #print(df_data_meas)
    #print(df_data_history_mod.info(memory_usage='deep'))
    #print(df_data_history_mod)

    return df_data_meas, df_data_history_mod

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

def read_data(name_table):
    """
    Lectura de archivos parquet en la carpeta broze
    :param name_table: nombre de archivo a leer (meas/history)
    :return df_measure : dataframe con datos de columnas relevantes y encabezados renbombrados
    """
    # Lectura de archivos parquet
    df= pd.read_parquet(
        path=f"{dir_bronze}/{name_table}",
        engine="fastparquet",
        )

    return df

# ----- Funciones para interactuar con base de datos ------ #
def create_table_db(engine):
    """
    Creo estructura para base de datos si no existiese
    :param engine: cursor de conexion a base de datos
    """

    # **** NOTA : Cuidado con las mayusculas y minusculas ****
    # Defino esquema de tabla para base de datos para meas
    query = text(f"""
    -- Creamos una tabla sobre datos 
    CREATE TABLE IF NOT EXISTS public.clima_v3_meas (
        id SERIAL PRIMARY KEY,
        localidad VARCHAR(255),
        region VARCHAR(255),
        pais VARCHAR(255),
        local_time TIMESTAMP,
        latitud INTEGER,
        longitud INTEGER,
        cond_climatica VARCHAR(255)
    );
    """)

    # Defino esquema de tabla para base de datos para history
    query2 = text(f"""
    -- Creamos una tabla sobre datos 
    CREATE TABLE IF NOT EXISTS public.clima_v3_history (
        id SERIAL PRIMARY KEY,
        localidad VARCHAR(255),
        fecha TIMESTAMP,
        hora INTEGER,
        temp_c FLOAT,
        presion_mb FLOAT,
        precip_mm FLOAT,
        humedad FLOAT
    );
    """)

    # Ejecutamos la query
    with engine.connect() as conn, conn.begin():
        conn.execute(query)

    # Ejecutamos la query
    with engine.connect() as conn, conn.begin():
        conn.execute(query2)

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

def read_db(engine):
    """
    Lectura de base de datos para testear

    :param engine cursor de conexion a base de datos
    """
    query = text(f"""
    SELECT *
    FROM public.clima_v3_meas
    """)

    query2 = text(f"""
    SELECT *
    FROM public.clima_v3_history
    """)

    with engine.connect() as conn, conn.begin():
        df_meas_check = pd.read_sql(query, conn)

    with engine.connect() as conn, conn.begin():
        df_history_check = pd.read_sql(query2, conn)

    print("Dataframe leido de la base datos:")
    print(df_meas_check)
    print(df_history_check)

# ------  Ejecucion de programa  ------ #
# Obtengo contraseña
access_token = get_token()

# EXTRACCIÓN FULL DE API
json_data_meas = multi_query(access_token, ENPOINT_MEAS_NOW)
# Normalizo en formato de dataframes
df_data_meas = build_table(json_data_meas)
df_data_meas["current.last_updated"] = pd.to_datetime(df_data_meas["current.last_updated"])
print("Dataframe obtenido de extracción full")
print(df_data_meas)  # DATAFRAME LISTO A GUARDAR COMO PARQUET Y LUEGO TRANSFORMAR Y GUARDAR EN BASE DE DATOS

# Secciono por localidad
# Guardo en formato parquet
save_to_parquet(
    df=df_data_meas,
    output_path=f"{dir_bronze}/meas/data.parquet",
    partition_cols=["location.name"]
    )

# EXTRACCIÓN INCREMENTAL DE API
# Obtengo rango de horario para solicitar a API (en este caso se realiza filtrando)
# Se considera el horario de Bs.As por eso se restan 3hs
start_time = datetime.utcnow() - timedelta(hours=3) - timedelta(minutes=5)
end_time = start_time + timedelta(hours=1)
# Obtengo fecha para solicitar a API
date_df = start_time.date()

# Consulto a API por varias localidades en la fecha actual
json_data_history = multi_query(access_token, ENPOINT_HISTORY, date_df)  # La API solo reconoce fecha, no el horario
df_data_history = build_table(json_data_history)

# Debido a limitacion de API en interpretar horario, se tranforma datos para obtener un
# unico dataframe y visualizar correctamente los datos en el horario solitado
df_data_history_mod = concat_data_API(df_data_history, start_time, end_time)
# Realizo extraccion incremental
df_data_history_update = extract_incremental_data(df_data_history_mod, start_time, end_time)
print("Dataframe obtenido de extracción incremental")
print(df_data_history_update)

# Considerando que la API (con el filtrado) solo entregara aquellos datos
# actualizados, el dataframe df_data_history_mod se va modificar por localidad
# unicamente en el caso que se cuente con un nuevo time de actualizacion,
# este obtenido la extraccion incremental
if not df_data_history_update.empty:
    # Establezco 'location.name' como índice
    df_data_history_mod.set_index('location.name', inplace=True)
    df_data_history_update.set_index('location.name', inplace=True)

    # Actualizo las filas del primer DataFrame con las filas del segundo DataFrame cuando haya coincidencias
    df_data_history_mod.update(df_data_history_update)

    # Restablezco indices
    df_data_history_mod.reset_index(inplace=True)

print("Dataframe obtenido al actualizar dataframe de API")
print(df_data_history_mod)

# Secciono por fecha y hora
# Convierto en formato de datetime columnas de fecha y hora
df_data_history_mod["time"] = pd.to_datetime(df_data_history_mod.time)
df_data_history_mod["update_date"] = df_data_history_mod.time.dt.date
df_data_history_mod["update_hour"] = df_data_history_mod.time.dt.hour

# Guardo en formato parquet
save_to_parquet(
    df=df_data_history_mod,
    output_path=f"{dir_bronze}/history/data.parquet",
    partition_cols=["update_date", "update_hour"]
    )

# TRANSFORMACION
df_data_meas = read_data("meas")
df_data_history_mod = read_data("history")

# Filtro columnas relevantes
df_data_meas, df_data_history_mod = filtro_columnas(df_data_meas, df_data_history_mod)

# Debido a que no se registro que haya errores relevantes en las extracciones
# de la API, se generan error forzados random y luego se aplican transformaciones
# ACLARACION: Esto unicamente para poder visualizar los datos erroneos con facilidad
#df_data_meas, df_data_history_mod = generated_error_random(df_data_meas, df_data_history_mod)

print("luego de errores")
print(df_data_meas)
print(df_data_history_mod)

print(df_data_meas.info(memory_usage='deep'))
print(df_data_history_mod.info(memory_usage='deep'))

# Elimino nulos e imputo valores
df_data_meas, df_data_history_mod = manejo_nulos(df_data_meas, df_data_history_mod)

# Asigno formato a las columnas de cada dataframe
df_data_meas, df_data_history_mod = manejo_formats(df_data_meas, df_data_history_mod)

# Manejo de duplicados
df_data_meas, df_data_history_mod = manejo_duplicados(df_data_meas, df_data_history_mod)

print("luego de transformaciones")
print(df_data_meas)
print(df_data_history_mod)

print(df_data_meas.info(memory_usage='deep'))
print(df_data_history_mod.info(memory_usage='deep'))

# CARGA EN BASE DE DATOS
# Acceso a base de datos postgre
engine = connect_to_db(
    "pipeline.conf",
    "postgres",
    "postgresql+psycopg2"
    )

# Creo esquema de tabla  - solo se crea si no existe
create_table_db(engine)
# Carga de Dataframe a base de datos meas
load_to_db(engine, df_data_meas, "clima_v3_meas")
# Carga de Dataframe a base de datos history
load_to_db(engine, df_data_history_mod, "clima_v3_history")
# Test load base de datos
read_db(engine)

# Guarda en formato parquet dataframe transformados
save_to_parquet(
    df=df_data_meas,
    output_path=f"{dir_silver}/meas/data.parquet",
    partition_cols=["localidad"]
    )

# para no tener problemas para crear carpeta
# estar en formato datetime considera el formato fecha y hora
# este no se puede usar para crear la carpeta silver
# por eso extraigo unicamente la fecha
df_data_history_mod["fecha"] = df_data_history_mod.fecha.dt.date

save_to_parquet(
    df=df_data_history_mod,
    output_path=f"{dir_silver}/history/data.parquet",
    partition_cols=["fecha", "hora"]
    )

