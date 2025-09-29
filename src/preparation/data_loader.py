# src/preparation/data_loader.py

import pandas as pd
from datetime import datetime
import os
import json # Necesitamos la librería json para manejar esta estructura

def load_and_filter_daily_files(execution_date_str: str, base_data_path: str = 'data') -> pd.DataFrame:
    """
    Carga, transforma y filtra los archivos del día desde el files.json correspondiente.

    Args:
        execution_date_str (str): La fecha de ejecución en formato 'YYYY-MM-DD'.
        base_data_path (str): La ruta a la carpeta principal de datos.

    Returns:
        pd.DataFrame: Un DataFrame con los archivos subidos en la fecha de ejecución.
                      Las columnas incluyen la información del archivo y el 'source_id'.
                      Retorna un DataFrame vacío si no hay archivos o si el archivo no existe.
    """
    print(f"--- Iniciando carga de 'files.json' para la fecha: {execution_date_str} ---")

    # 1. Construir la ruta al archivo
    file_path = os.path.join(base_data_path, f"{execution_date_str}_20_00_UTC", 'files.json')

    # 2. Leer y parsear el archivo JSON
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"!! ERROR: No se encontró el archivo: {file_path}")
        return pd.DataFrame() # Devolver un DataFrame vacío si el archivo no existe
    except json.JSONDecodeError:
        print(f"!! ERROR: El archivo {file_path} no es un JSON válido.")
        return pd.DataFrame()

    # 3. Transformar la estructura de Diccionario a Lista (aplanamiento)
    # El JSON es un diccionario {source_id: [lista_de_archivos]}. 
    # Necesitamos convertirlo a una lista de diccionarios, añadiendo el source_id a cada uno.
    all_files_list = []
    for source_id, files in data.items():
        for file_record in files:
            file_record['source_id'] = source_id # Añadimos el ID de la fuente a cada registro
            all_files_list.append(file_record)
    
    if not all_files_list:
        print("-> El archivo JSON está vacío. No hay archivos para procesar.")
        return pd.DataFrame()

    # 4. Convertir la lista a un DataFrame
    df = pd.DataFrame(all_files_list)
    print(f"✓ Se transformaron {len(df)} registros totales desde el JSON.")

    # 5. Filtrar el DataFrame por la fecha de ejecución
    # Convertimos 'uploaded_at' a tipo datetime para poder comparar fechas
    df['uploaded_at'] = pd.to_datetime(df['uploaded_at'])
    
    # Obtenemos la fecha de ejecución como un objeto 'date'
    execution_date = datetime.strptime(execution_date_str, '%Y-%m-%d').date()

    # Filtramos manteniendo solo las filas cuya fecha en 'uploaded_at' coincide
    df_filtered = df[df['uploaded_at'].dt.date == execution_date].copy()
    
    print(f"✓ Se filtraron {len(df_filtered)} archivos que corresponden a la fecha {execution_date_str}.")
    print("--- Proceso de carga y filtrado finalizado. ---")

    return df_filtered

def create_historical_summary(base_data_path: str = 'data') -> pd.DataFrame:
    """
    Crea un DataFrame histórico agregado por día y fuente a partir de todos los
    archivos 'files.json' y 'files_last_weekday.json' disponibles.

    Args:
        base_data_path (str): La ruta a la carpeta principal de datos.

    Returns:
        pd.DataFrame: Un DataFrame con estadísticas agregadas por día y fuente.
    """
    print("\n--- Iniciando la creación del resumen histórico ---")
    
    # ETAPA 1: RECOLECCIÓN TOTAL
    # --------------------------
    all_files_list = []
    
    # Identificar todas las carpetas de fechas en el directorio de datos
    try:
        date_folders = [d for d in os.listdir(base_data_path) if os.path.isdir(os.path.join(base_data_path, d))]
    except FileNotFoundError:
        print(f"!! ERROR: El directorio base '{base_data_path}' no fue encontrado.")
        return pd.DataFrame()

    print(f"Se encontraron {len(date_folders)} carpetas de fechas para procesar.")

    for folder in date_folders:
        for filename in ['files.json', 'files_last_weekday.json']:
            file_path = os.path.join(base_data_path, folder, filename)
            
            if not os.path.exists(file_path):
                continue # Si el archivo no existe, simplemente lo saltamos

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Aplanamos el JSON, añadiendo el source_id (misma lógica que antes)
                for source_id, files in data.items():
                    for file_record in files:
                        file_record['source_id'] = source_id
                        all_files_list.append(file_record)
            except (json.JSONDecodeError, FileNotFoundError):
                print(f"!! ADVERTENCIA: No se pudo procesar el archivo {file_path}. Saltando.")
                continue
    
    if not all_files_list:
        print("!! ERROR: No se encontraron datos en ninguna de las fuentes. Finalizando.")
        return pd.DataFrame()
        
    # ETAPA 2: CONSOLIDACIÓN Y LIMPIEZA
    # ---------------------------------
    df_consolidated = pd.DataFrame(all_files_list)
    print(f"Se cargaron {len(df_consolidated)} registros en total.")
    
    # Eliminar duplicados donde la fila entera es idéntica
    df_consolidated.drop_duplicates(inplace=True)
    print(f"Quedan {len(df_consolidated)} registros después de eliminar duplicados exactos.")
    
    # ETAPA 3: INGENIERÍA DE CARACTERÍSTICAS Y AGREGACIÓN
    # ----------------------------------------------------
    print("Iniciando ingeniería de características para la agregación...")

    # Convertir a datetime y extraer componentes de fecha/hora
    df_consolidated['uploaded_at'] = pd.to_datetime(df_consolidated['uploaded_at'])
    df_consolidated['uploaded_at_date'] = df_consolidated['uploaded_at'].dt.date
    df_consolidated['upload_hour'] = df_consolidated['uploaded_at'].dt.hour

    # Crear columnas booleanas para facilitar los cálculos condicionales
    df_consolidated['is_duplicated_stopped'] = (df_consolidated['is_duplicated'] == True) & (df_consolidated['status'] == 'stopped')
    df_consolidated['is_processed'] = df_consolidated['status'] == 'processed'
    df_consolidated['is_other_status'] = ~df_consolidated['status'].isin(['processed', 'stopped'])
    df_consolidated['is_filesize_null'] = df_consolidated['file_size'].isnull()
    df_consolidated['is_filesize_zero'] = df_consolidated['file_size'] == 0
    df_consolidated['is_filesize_positive'] = df_consolidated['file_size'] > 0
    df_consolidated['is_filename_duplicated_in_source'] = df_consolidated.duplicated(subset=['source_id', 'filename'], keep=False)

    print("Agrupando y calculando métricas principales...")
    
    # Agregación principal
    agg_dict = {
        'filename': ('filename', 'count'),
        'is_duplicated_stopped': ('is_duplicated_stopped', 'sum'),
        'is_duplicated': ('is_duplicated', 'sum'),
        'is_processed': ('is_processed', 'sum'),
        'is_other_status': ('is_other_status', 'sum'),
        'file_size': ('file_size', 'sum'),
        'rows': ('rows', 'sum'),
        'is_filesize_null': ('is_filesize_null', 'sum'),
        'is_filesize_zero': ('is_filesize_zero', 'sum'),
        'is_filesize_positive': ('is_filesize_positive', 'sum'),
        'is_filename_duplicated_in_source': ('is_filename_duplicated_in_source', 'sum')
    }

    df_summary = df_consolidated.groupby(['uploaded_at_date', 'source_id']).agg(**agg_dict)

    # Renombrar columnas para mayor claridad
    df_summary.rename(columns={
        'filename': 'total_files',
        'is_duplicated_stopped': 'total_files_duplicated_stopped',
        'is_duplicated': 'total_files_duplicated',
        'is_processed': 'total_files_processed',
        'is_other_status': 'total_files_other_status',
        'file_size': 'sum_file_size',
        'rows': 'sum_rows',
        'is_filesize_null': 'total_files_filesize_null',
        'is_filesize_zero': 'total_files_filesize_zero',
        'is_filesize_positive': 'total_files_filesize_positive',
        'is_filename_duplicated_in_source': 'total_filename_duplicated_in_source'
    }, inplace=True)

    # Agregación por hora (usando pivot_table)
    print("Calculando métricas por hora...")
    
    # Conteo de archivos por hora
    pivot_counts = df_consolidated.pivot_table(
        index=['uploaded_at_date', 'source_id'],
        columns='upload_hour',
        values='filename',
        aggfunc='count',
        fill_value=0
    ).add_prefix('total_files_h')

    # Suma de file_size por hora
    pivot_size = df_consolidated.pivot_table(
        index=['uploaded_at_date', 'source_id'],
        columns='upload_hour',
        values='file_size',
        aggfunc='sum',
        fill_value=0
    ).add_prefix('sum_filesize_h')

    # Suma de rows por hora
    pivot_rows = df_consolidated.pivot_table(
        index=['uploaded_at_date', 'source_id'],
        columns='upload_hour',
        values='rows',
        aggfunc='sum',
        fill_value=0
    ).add_prefix('sum_rows_h')

    # Unir los resultados del pivot con el resumen principal
    print("Uniendo todas las métricas...")
    df_final_summary = df_summary.join(pivot_counts).join(pivot_size).join(pivot_rows).reset_index()

    print("--- Resumen histórico creado exitosamente. ---")
    return df_final_summary

def load_feedback_data(base_data_path: str = 'data') -> pd.DataFrame:
    """
    Carga los datos de feedback desde el archivo Excel.

    Args:
        base_data_path (str): La ruta a la carpeta principal de datos.

    Returns:
        pd.DataFrame: Un DataFrame con los datos de feedback.
                      Retorna un DataFrame vacío si el archivo no existe.
    """
    print("\n--- Iniciando la carga del archivo de feedback ---")
    
    # Construir la ruta al archivo Excel de feedback
    feedback_file_path = os.path.join(base_data_path, 'Feedback - week 9 sept.xlsx')
    
    try:
        # Usamos pd.read_excel para leer el archivo .xlsx
        df = pd.read_excel(feedback_file_path, engine='openpyxl')
        print(f"✓ Se cargaron {len(df)} registros desde '{feedback_file_path}'.")
        return df
    except FileNotFoundError:
        print(f"!! ERROR: No se encontró el archivo de feedback en: {feedback_file_path}")
        return pd.DataFrame()
    except Exception as e:
        # Captura otros posibles errores, por ejemplo, si openpyxl no está instalado
        print(f"!! ERROR: Ocurrió un error al leer el archivo Excel: {e}")
        return pd.DataFrame()

# --- Bloque de prueba ---
if __name__ == '__main__':
    # --- Prueba para la Función 1 ---
    TEST_DATE = '2025-09-08' 
    print(f"************ PRUEBA 1: Cargando datos para el día {TEST_DATE} ************")
    df_files_operation_date = load_and_filter_daily_files(TEST_DATE)
    
    print("\n--- Verificación de df_files_operation_date ---")
    if not df_files_operation_date.empty:
        print("✓ El DataFrame de operación diaria se cargó correctamente.")
        print(f"  Dimensiones: {df_files_operation_date.shape}")
    else:
        print("✗ El DataFrame de operación diaria está vacío.")

    # --- Prueba para la Función 2 ---
    print(f"\n************ PRUEBA 2: Creando el resumen histórico ************")
    df_historical_summary = create_historical_summary()

    print("\n--- Verificación de df_historical_summary ---")
    if not df_historical_summary.empty:
        print("✓ El DataFrame de resumen histórico se creó correctamente.")
        print(f"  Dimensiones: {df_historical_summary.shape}")
    else:
        print("✗ El DataFrame de resumen histórico está vacío.")

    # --- Prueba para la Función 3 ---
    print(f"\n************ PRUEBA 3: Cargando el archivo de feedback ************")
    df_feedback = load_feedback_data()

    print("\n--- Verificación de df_feedback ---")
    if not df_feedback.empty:
        print("✓ El DataFrame de feedback se cargó correctamente.")
        print(f"  Dimensiones: {df_feedback.shape}")
        print("\nPrimeras 5 filas del feedback:")
        print(df_feedback.head())
    else:
        print("✗ El DataFrame de feedback está vacío.")