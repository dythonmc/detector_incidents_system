import pandas as pd
from datetime import datetime

def detect_duplicated_and_failed_files(df_source_files: pd.DataFrame, verbose: bool = True) -> list:
    """
    Identifica archivos duplicados o fallidos y devuelve un único objeto de incidencia si se encuentran.
    """
    if df_source_files is None or df_source_files.empty:
        return []

    incident_mask = (df_source_files['is_duplicated'] == True) | \
                    (df_source_files['status'].str.lower() == 'stopped')

    df_incidents = df_source_files[incident_mask]

    if verbose and df_incidents.empty:
        print("     -> [LOG] No se encontraron archivos marcados como 'is_duplicated' o con estado 'stopped'.")
        return []

    if not df_incidents.empty:
        incident_object = {
            "source_id": str(df_incidents.iloc[0]['source_id']),
            "incident_type": "Archivo Duplicado o Fallido",
            "incident_details": f"Se encontraron {len(df_incidents)} archivos marcados como duplicados o con estado 'stopped'.",
            "total_incidentes": len(df_incidents), # <-- NUEVO CAMPO
            "files_to_review": df_incidents['filename'].tolist()
        }
        return [incident_object]

    return []

def detect_unexpected_empty_files(
    df_source_files: pd.DataFrame, 
    source_cv_info: dict, 
    operation_date_str: str, 
    verbose: bool = True
) -> list:
    """
    Identifica archivos vacíos inesperados y devuelve un único objeto de incidencia si se encuentran.
    """
    if df_source_files is None or df_source_files.empty:
        if verbose:
            print("     -> [LOG] No se recibieron archivos para esta fuente hoy.")
        return []

    df_empty_files = df_source_files[df_source_files['rows'] == 0].copy()
    
    if df_empty_files.empty:
        if verbose:
            print("     -> [LOG] No se encontraron archivos con 0 filas para esta fuente hoy.")
        return []

    if not source_cv_info:
        if verbose:
            print("     -> [LOG] No hay datos de CV para esta fuente. Marcando archivos vacíos como incidencia por precaución.")
        incident_object = {
            "source_id": str(df_empty_files.iloc[0]['source_id']),
            "incident_type": "Archivo Vacío Inesperado",
            "incident_details": f"Se recibieron {len(df_empty_files)} archivos vacíos y no hay CV para verificar si es un patrón normal.",
            "total_incidentes": len(df_empty_files), # <-- NUEVO CAMPO
            "files_to_review": df_empty_files['filename'].tolist()
        }
        return [incident_object]

    today_empty_count = len(df_empty_files)
    is_incident = False
    details = ""

    operation_date = datetime.strptime(operation_date_str, '%Y-%m-%d')
    day_abbr = operation_date.strftime('%a')

    day_stats_list = source_cv_info.get("day_of_week_row_stats", [])
    day_stats = next((d for d in day_stats_list if d.get('day') == day_abbr), None)

    if day_stats and day_stats.get('empty_files_mean') is not None:
        mean_empty = day_stats['empty_files_mean']
        if today_empty_count > round(mean_empty) + 1:
            is_incident = True
            details = f"Se recibieron {today_empty_count} archivos vacíos, superando la media histórica de ~{mean_empty:.2f} para los {day_abbr}."
        elif verbose:
            print(f"     -> [LOG] Se encontraron {today_empty_count} archivos vacíos, lo cual es consistente con la media de {mean_empty:.2f} para los {day_abbr}.")
    else:
        if verbose:
            print("     -> [LOG] No se encontró 'empty_files_mean' para el día. Usando lógica de fallback (median_rows).")
        general_stats = source_cv_info.get("general_volume_stats", {})
        median_rows = general_stats.get("median_rows")
        if median_rows is not None and median_rows > 50:
            is_incident = True
            details = f"Se recibieron {today_empty_count} archivos vacíos. La mediana de filas para esta fuente es {median_rows}, por lo que no se esperan archivos vacíos."
        elif verbose:
            details_log = f"mediana de {median_rows}" if median_rows is not None else "sin datos de mediana"
            print(f"     -> [LOG] Archivos vacíos no se marcan como incidencia basado en la lógica de fallback ({details_log}).")

    if is_incident:
        incident_object = {
            "source_id": str(df_empty_files.iloc[0]['source_id']),
            "incident_type": "Archivo Vacío Inesperado",
            "incident_details": details,
            "total_incidentes": len(df_empty_files), # <-- NUEVO CAMPO
            "files_to_review": df_empty_files['filename'].tolist()
        }
        return [incident_object]
    
    return []