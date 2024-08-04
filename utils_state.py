import json
from datetime import datetime, date

def read_state_from_json(file_path):
    """
    Lee un archivo JSON que contiene el ultimo valor incremental extraído
    de cada tabla de la base de datos.

    Parámetros:
        file_path (str): Ruta del archivo JSON

    Retorna:
        Diccionario con el contenido del archivo JSON
    """
    try:
        with open(file_path, 'r') as file:
            state = json.load(file)
            return state
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo JSON en la ruta {file_path} no existe.")
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"El archivo JSON en la ruta {file_path} no es válido.")
    
def write_state_to_json(file_path, state):
    """
    Escribe el estado de la replicación en un archivo JSON
    
    Parámetros:
        file_path (str): Ruta del archivo JSON
        state (dict): Objeto con el estado de la replicación
    """
    try:
        with open(file_path, 'w') as file:
            json.dump(state, file, default=str, indent=4)
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo JSON en la ruta {file_path} no existe.")
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"El archivo JSON en la ruta {file_path} no es válido.")

def get_last_incremental_value(state, table_name):
    """
    Obtiene el último valor incremental de una tabla

    Parámetros:
        state (dict): Objeto con el estado de la replicación
        table_name (str): Nombre de la tabla

    Retorna:
        Ultimo valor incremental de la tabla. Debe ser date, datetime, timestamp.
    """
    try:
        return state[table_name]['last_value']
    except KeyError:
        raise KeyError(f"La tabla {table_name} no existe en el archivo JSON.")
    
def update_incremental_value(state, file_path, table_name, new_value):
    """
    Actualiza el valor incremental de una tabla en el estado de la replicación

    Parámetros:
        state (dict): Objeto con el estado de la replicación
        file_path (str): Ruta donde guardar el archivo JSON
        table_name (str): Nombre de la tabla
        new_value: Nuevo valor incremental. Puede ser date, datetime, timestamp.
    """

    last_incremental_value = get_last_incremental_value(state, table_name)
    
    # Chequeamos el tipo de datos
    if isinstance(new_value, datetime):
        last_incremental_value = datetime.fromisoformat(last_incremental_value)
    else:
        raise TypeError(f"El tipo de dato {type(new_value)} no está soportado. Debe ser date, datetime o timestamp.")

    if new_value < last_incremental_value:
        raise ValueError(f"El nuevo valor incremental {new_value} es menor al valor anterior {last_incremental_value}.")
    elif new_value is None:
        raise ValueError(f"El nuevo valor incremental {new_value} no puede ser nulo.")
    
    # Actualizamos el valor incremental
    state[table_name]['last_value'] = new_value
    write_state_to_json(file_path, state)