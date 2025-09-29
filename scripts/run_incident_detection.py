import os
import sys
import pandas as pd

# Añadimos la ruta raíz del proyecto al sys.path para que encuentre el módulo 'src'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Importamos las funciones que hemos creado
from src.preparation.data_loader import load_and_filter_daily_files
from src.detection.detectors import detect_duplicated_and_failed_files

# --- CONFIGURACIÓN ---
# Define para qué día quieres correr la detección
OPERATION_DATE = "2025-09-08"
OUTPUT_DIR = "outputs"

def main():
    """
    Script principal para orquestar la detección de incidencias para una fecha de operación.
    """
    print(f"--- Iniciando Detección de Incidencias para el día: {OPERATION_DATE} ---")

    # --- 1. FASE DE CARGA DE DATOS ---
    print("\n[1/3] Cargando datos de operación...")
    df_files_operation_date = load_and_filter_daily_files(OPERATION_DATE)

    if df_files_operation_date is None or df_files_operation_date.empty:
        print("!! No se encontraron datos de operación para la fecha especificada. Terminando proceso.")
        return

    print(f"✓ Datos cargados: {len(df_files_operation_date)} archivos procesados el {OPERATION_DATE}.")
    
    # (Aquí cargaremos el cv_data.json y otros DFs cuando los necesitemos para futuros detectores)

    # --- 2. FASE DE DETECCIÓN ---
    print("\n[2/3] Ejecutando detectores...")
    
    # Lista para almacenar los resultados de todos los detectores
    all_incidents = []

    # Detector 1: Archivos duplicados y fallidos
    print("  -> Ejecutando: Detección de archivos duplicados y fallidos...")
    df_incidents_duplicated = detect_duplicated_and_failed_files(df_files_operation_date)
    if not df_incidents_duplicated.empty:
        print(f"     -> Se encontraron {len(df_incidents_duplicated)} incidencias.")
        all_incidents.append(df_incidents_duplicated)
    else:
        print("     -> No se encontraron incidencias de este tipo.")

    # (Aquí llamaremos a los otros detectores que vayamos construyendo)
    # df_incidents_missing = detect_missing_files(...)
    # all_incidents.append(df_incidents_missing)

    # --- 3. FASE DE REPORTE ---
    print("\n[3/3] Consolidando y guardando el reporte de incidencias...")

    if not all_incidents:
        print("¡Excelente! No se encontraron incidencias de ningún tipo para esta fecha.")
        return
    
    # Consolidar todos los DataFrames de incidencias en uno solo
    df_final_report = pd.concat(all_incidents, ignore_index=True)

    # Organizar columnas para mayor claridad en el reporte
    report_columns = [
        'source_id',
        'incident_type',
        'incident_details',
        'filename',
        'uploaded_at',
        'status',
        'is_duplicated',
        'rows',
        'file_size'
    ]
    # Filtramos para quedarnos solo con las columnas que existen en el DF
    final_columns = [col for col in report_columns if col in df_final_report.columns]
    df_final_report = df_final_report[final_columns]

    print(f"\n--- REPORTE FINAL: Se encontraron un total de {len(df_final_report)} incidencias. ---")
    print(df_final_report[['source_id', 'incident_type', 'incident_details', 'filename']].head())

    # Guardar el reporte en un archivo CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{OPERATION_DATE}_incidents_report.csv")
    df_final_report.to_csv(output_path, index=False)
    
    print(f"\n✓ Reporte de incidencias guardado exitosamente en: {output_path}")


if __name__ == '__main__':
    main()