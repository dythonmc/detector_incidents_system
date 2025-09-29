import pandas as pd

def detect_duplicated_and_failed_files(df_files_operation_date: pd.DataFrame) -> pd.DataFrame:
    """
    Identifica archivos que están marcados como duplicados o que fallaron en su procesamiento.
    Un archivo duplicado es identificado cuando is_duplicated = TRUE y status = STOPPED 
    o simplemente porque contiene el mismo nombre de archivo.

    Args:
        df_files_operation_date (pd.DataFrame): DataFrame con los archivos del día de operación.

    Returns:
        pd.DataFrame: Un DataFrame que contiene solo las filas de los archivos con incidencias,
                      con una nueva columna 'incident_type' y 'incident_details'.
    """
    if df_files_operation_date is None or df_files_operation_date.empty:
        return pd.DataFrame()

    # Criterio 1: is_duplicated es True O status es 'stopped'
    # Usamos el operador | (OR) para combinar las condiciones
    incident_mask = (df_files_operation_date['is_duplicated'] == True) | \
                    (df_files_operation_date['status'].str.lower() == 'stopped')

    df_incidents = df_files_operation_date[incident_mask].copy()

    if not df_incidents.empty:
        # Añadir columnas para el reporte consolidado
        df_incidents['incident_type'] = 'Archivo Duplicado o Fallido'
        
        # Crear detalles específicos basados en la condición que se cumplió
        details = []
        for index, row in df_incidents.iterrows():
            detail_parts = []
            if row['is_duplicated']:
                detail_parts.append("marcado como duplicado")
            if str(row['status']).lower() == 'stopped':
                detail_parts.append(f"estado es 'stopped'")
            details.append(f"El archivo está {', y '.join(detail_parts)}.")
        
        df_incidents['incident_details'] = details

    return df_incidents