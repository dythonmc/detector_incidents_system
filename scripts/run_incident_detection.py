import os
import sys
import json

# Añadimos la ruta raíz del proyecto al sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Importamos las funciones que hemos creado
from src.preparation.data_loader import load_and_filter_daily_files
from src.detection.detectors import (
    detect_duplicated_and_failed_files,
    detect_unexpected_empty_files
)

# --- CONFIGURACIÓN ---
OPERATION_DATE = "2025-09-08"
OUTPUT_DIR = "outputs"
CV_DATA_PATH = os.path.join(OUTPUT_DIR, "cv_data.json")

def main():
    """
    Script principal para orquestar la detección de incidencias, iterando por cada
    fuente y generando un reporte JSON consolidado.
    """
    print(f"--- Iniciando Detección de Incidencias para el día: {OPERATION_DATE} ---")

    # --- 1. FASE DE CARGA DE DATOS ---
    print("\n[1/3] Cargando todos los datos necesarios...")
    df_files_operation_date = load_and_filter_daily_files(OPERATION_DATE)
    print(f"✓ Datos de operación cargados: {len(df_files_operation_date)} archivos procesados el {OPERATION_DATE}.")

    try:
        with open(CV_DATA_PATH, 'r') as f:
            cv_data = json.load(f)
        print(f"✓ Datos de inteligencia de CVs cargados. {len(cv_data)} fuentes a analizar.")
    except FileNotFoundError:
        print(f"!! ERROR: No se encontró el archivo '{CV_DATA_PATH}'. Ejecuta primero 'run_data_mining.py'.")
        return

    # --- 2. FASE DE DETECCIÓN ---
    print("\n[2/3] Ejecutando detectores para cada fuente...")
    
    all_source_ids = [str(item.get('source_id')) for item in cv_data]
    all_incidents = [] # Esta lista ahora contendrá los 'objetos de incidencia'

    for source_id in all_source_ids:
        print(f"\n--- Analizando Fuente: {source_id} ---")
        df_source_files = df_files_operation_date[df_files_operation_date['source_id'] == source_id].copy()
        source_cv_info = next((item for item in cv_data if str(item.get('source_id')) == source_id), None)
        
        # Detector 1: Archivos duplicados y fallidos
        incidents_dup_fail = detect_duplicated_and_failed_files(df_source_files)
        if incidents_dup_fail:
            print(f"     -> ¡INCIDENCIA ENCONTRADA!: 'Duplicados o Fallidos'.")
            all_incidents.extend(incidents_dup_fail) # Usamos extend para añadir los elementos de la lista

        # Detector 2: Archivos vacíos inesperados
        incidents_empty = detect_unexpected_empty_files(df_source_files, source_cv_info, OPERATION_DATE)
        if incidents_empty:
            print(f"     -> ¡INCIDENCIA ENCONTRADA!: 'Vacíos Inesperados'.")
            all_incidents.extend(incidents_empty)

    # --- 3. FASE DE REPORTE ---
    print("\n\n[3/3] Consolidando y guardando el reporte de incidencias...")
    if not all_incidents:
        print("¡Excelente! No se encontraron incidencias de ningún tipo para esta fecha.")
        return
    
    print(f"\n--- REPORTE FINAL: Se encontraron un total de {len(all_incidents)} tipos de incidencias. ---")
    
    # Guardar el reporte en un archivo JSON
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{OPERATION_DATE}_incidents_report.json") # <--- CAMBIO a .json
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_incidents, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Reporte de incidencias guardado exitosamente en: {output_path}")

if __name__ == '__main__':
    main()