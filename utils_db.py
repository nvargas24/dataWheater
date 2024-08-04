import os
from configparser import ConfigParser

import pandas as pd
from sqlalchemy import MetaData, create_engine, text

from utils_state import *


def connect_to_db(config_file, section, driverdb):
    """
    Crea una conexión a la base de datos especificada en el archivo de configuración.

    Parámetros:
    config_file (str): La ruta del archivo de configuración.
    section (str): La sección del archivo de configuración que contiene los datos de la base de datos.
    driverdb (str): El driver de la base de datos a la que se conectará.

    Retorna:
    Un objeto de conexión a la base de datos.
    """
    try:
        # Lectura del archivo de configuración
        parser = ConfigParser()
        parser.read(config_file)

        # Creación de un diccionario
        # donde cargaremos los parámetros de la base de datos
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            db = {param[0]: param[1] for param in params}

            # Creación de la conexión a la base de datos
            engine = create_engine(
                f"{driverdb}://{db['user']}:{db['pwd']}@{db['host']}:{db['port']}/{db['dbname']}"
            )
            return engine

        else:
            print(
                f"Sección {section} no encontrada en el archivo de configuración.")
            return None
    except Exception as e:
        print(f"Error al conectarse a la base de datos: {e}")
        return None


def get_metadata_db(sqlalchemy_engine):
    """
    Genera un archivo JSON con la metadata de la base de datos,
    con el formato
    {
        table_name: {
            column_name: {column_metadata}
                    }
    }

    Parámetros:
        sqlalchemy_engine: Objeto de conexión de SQLAlchemy

    Retorna:
        None
    """

    try:
        # Establecer conexión con la metadata de la base de datos
        metadata = MetaData()
        metadata.reflect(bind=sqlalchemy_engine)
    except Exception as e:
        print(f"Error al conectar con la base de datos: {e}")
        return

    # Crear un diccionario donde se almacenará la metadata
    metadata_dict = {}
    for tbl in metadata.tables.values():
        table_dict = {}
        for column in tbl.c:
            col_dict = {
                'type': str(column.type),
                'nullable': column.nullable,
                'default': column.default,
                'primary_key': column.primary_key,
                'references': ''.join([str(fk.column) for fk in column.foreign_keys])
            }
            table_dict[column.name] = col_dict
        metadata_dict[tbl.name] = table_dict

    try:
        # Guardar el diccionario en un archivo JSON
        os.makedirs('metadata', exist_ok=True)
        metadata_obj = json.dumps(metadata_dict, indent=4)
        with open('metadata/metadata_tables.json', 'w') as file:
            file.write(metadata_obj)
    except Exception as e:
        print(f"Error al guardar el archivo JSON: {e}")


def get_columns_from_table(table_name):
    """
    Obtiene las columnas de una tabla a partir del archivo de metadata.

    Parámetros:
    - table_name (str): Nombre de la tabla.

    Retorna:
    - Un string con los nombres de las columnas separados por comas.
    """

    try:
        with open('metadata/metadata_tables.json', 'r') as file:
            metadata = json.load(file)
    except Exception as e:
        print(f"Error al cargar el archivo JSON: {e}")
        return

    columns = list(metadata.get(table_name, {}).keys())
    if not columns:
        print(
            f"No se encontraron columnas para la tabla {table_name} en el archivo JSON.")
        return
    columns_str = ', '.join(columns)
    return columns_str


def extract_full_data(sqlalchemy_engine, table_name):
    """
    Extracción FULL de datos desde una tabla de una base de datos SQL.

    Parámetros:
    - sqlalchemy_engine: Objeto de conexión de SQLAlchemy.
    - table_name (str): Nombre de la tabla desde la cual extraer los datos.

    Retorna:
    - Un DataFrame con todos los datos de la tabla.
    """

    # Obtener las columnas de la tabla a partir del archivo de metadata
    columns = get_columns_from_table(table_name)
    query = f"SELECT {columns} FROM {table_name}"
    with sqlalchemy_engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df


def extract_incremental_data(sqlalchemy_engine, table_name, state_file_path):
    """
    Extracción INCREMENTAL de datos desde una tabla de una base de datos SQL
    utilizando un archivo JSON para gestionar el ultimo valor incremental extraído.

    Parámetros:
    - sqlalchemy_engine: Objeto de conexión de SQLAlchemy.
    - table_name (str): Nombre de la tabla desde la cual extraer los datos.
    - state_file_path (str): Ruta del archivo JSON que contiene el estado de la replicación.

    Retorna:
    - Un DataFrame con los datos incrementales de la tabla.
    """
    # Obtener las columnas de la tabla a partir del archivo de metadata
    columns = get_columns_from_table(table_name)
    print("columns:" , columns)
    # Obtener la columna incremental y su último valor
    state = read_state_from_json(state_file_path)
    last_value = get_last_incremental_value(state, table_name)

    incremental_column = state[table_name]["incremental_column"]

    # Obtener los datos nuevos de la tabla
    query = text(f"SELECT {columns} FROM {table_name} WHERE {incremental_column} > '{last_value}'")
    with sqlalchemy_engine.connect() as conn:
        df = pd.read_sql_query(query, sqlalchemy_engine)

    if not df.empty:
        new_value = df[incremental_column].max()
        update_incremental_value(
            state, state_file_path, table_name, new_value)

    return df
