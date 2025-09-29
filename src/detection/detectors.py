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

def detect_unexpected_empty_files(df_files_operation_date: pd.DataFrame, cv_data: list) -> pd.DataFrame:
    """
    Identifica archivos vacíos (0 filas) que no son un patrón esperado para la fuente.

    Args:
        df_files_operation_date (pd.DataFrame): DataFrame con los archivos del día.
        cv_data (list): Una lista de diccionarios con los datos extraídos de los CVs.

    Returns:
        pd.DataFrame: Un DataFrame con las incidencias de archivos vacíos inesperados.
    """
    if df_files_operation_date is None or df_files_operation_date.empty:
        return pd.DataFrame()

    # 1. Filtrar candidatos: archivos con 0 filas hoy
    df_empty_files = df_files_operation_date[df_files_operation_date['rows'] == 0].copy()

    if df_empty_files.empty:
        return pd.DataFrame()

    # Convertir datos del CV a un diccionario para búsqueda rápida: {source_id: stats}
    cv_dict = {str(item.get('source_id')): item.get('general_volume_stats', {}) for item in cv_data}

    unexpected_empty_indices = []
    for index, row in df_empty_files.iterrows():
        source_id = str(row['source_id'])
        source_cv_stats = cv_dict.get(source_id)

        is_unexpected = False
        # 3. Aplicar la regla
        if source_cv_stats:
            pct_empty = source_cv_stats.get('pct_empty_files')
            # Si no hay datos (None) o el porcentaje es bajo, es inesperado.
            if pct_empty is None or pct_empty < 10.0:
                is_unexpected = True
        else:
            # Si no hay información del CV para esta fuente, lo marcamos por precaución.
            is_unexpected = True
        
        if is_unexpected:
            unexpected_empty_indices.append(index)

    df_incidents = df_empty_files.loc[unexpected_empty_indices].copy()

    if not df_incidents.empty:
        df_incidents['incident_type'] = 'Archivo Vacío Inesperado'
        df_incidents['incident_details'] = 'Se recibió un archivo con 0 filas, lo cual no es un patrón común para esta fuente según su CV.'

    return df_incidents