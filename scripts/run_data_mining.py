import os
import json
import re
import sys
import asyncio

# Añadimos la ruta raíz del proyecto al sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Importaciones requeridas por ADK
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types

# Importamos nuestra instancia de agente
from src.agents.data_miner.agent import data_miner_agent

# --- CONFIGURACIÓN ---
CV_FOLDER_PATH = "data/datasource_cvs"
OUTPUT_FILE_PATH = "outputs/cv_data.json"
APP_NAME = "data_miner_app"
USER_ID = "dev_user"

def clean_json_string(raw_string: str) -> str | None:
    """Limpia la respuesta del LLM para extraer solo el bloque JSON."""
    match = re.search(r'```json\s*(\{.*?\})\s*```', raw_string, re.DOTALL)
    if match:
        return match.group(1)
    match = re.search(r'(\{.*?\})', raw_string, re.DOTALL)
    if match:
        return match.group(1)
    return None

async def main():
    """Script principal para orquestar la minería de datos de todos los CVs."""
    print("--- Iniciando el Proceso de Minería de Datos de CVs (Patrón ADK Async Runner) ---")

    session_service = InMemorySessionService()
    runner = Runner(agent=data_miner_agent, app_name=APP_NAME, session_service=session_service)

    # --- PROCESAMIENTO COMPLETO: Procesar todos los archivos ---
    cv_files = [f for f in os.listdir(CV_FOLDER_PATH) if f.endswith('_native.md')]
    print(f"Se encontraron {len(cv_files)} archivos CV para procesar en total.")

    all_cv_data = []
    for cv_file in cv_files:
        file_path = os.path.join(CV_FOLDER_PATH, cv_file)
        if not os.path.exists(file_path):
            print(f"!! ADVERTENCIA: Saltando archivo no encontrado en '{file_path}'")
            continue

        source_id = cv_file.split('_')[0]
        session_id = f"session_{source_id}"
        
        print(f"\n--- Procesando CV para source_id: {source_id} ---")

        await session_service.create_session(app_name=APP_NAME, user_id=USER_ID, session_id=session_id)
        
        user_prompt = f"Por favor, lee el archivo usando la herramienta read_file_content con la ruta '{file_path}' y extrae la información estructurada como se te indicó en tus instrucciones."
        user_message = types.Content(role='user', parts=[types.Part(text=user_prompt)])

        try:
            final_response_text = None
            async for event in runner.run_async(user_id=USER_ID, session_id=session_id, new_message=user_message):
                if event.is_final_response() and event.content:
                    final_response_text = event.content.parts[0].text.strip()
            
            if final_response_text:
                json_string = clean_json_string(final_response_text)
                if json_string:
                    extracted_data = json.loads(json_string)
                    extracted_data['source_id'] = source_id
                    all_cv_data.append(extracted_data)
                    print(f"✓ Extracción exitosa para source_id: {source_id}")
                else:
                    print(f"!! ADVERTENCIA: No se pudo extraer un JSON válido para {source_id}.")
                    print("Respuesta final recibida:", final_response_text)
            else:
                print("!! ADVERTENCIA: El agente no produjo una respuesta final.")

        except Exception as e:
            print(f"!! ERROR: Ocurrió una excepción al procesar {source_id}: {e}")
            
    if all_cv_data:
        os.makedirs(os.path.dirname(OUTPUT_FILE_PATH), exist_ok=True)
        with open(OUTPUT_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(all_cv_data, f, indent=2, ensure_ascii=False)
        print(f"\n--- Proceso completado. {len(all_cv_data)} CVs procesados exitosamente. ---")
        print(f"✓ Los datos estructurados han sido guardados en: {OUTPUT_FILE_PATH}")
    else:
        print("\n--- Proceso completado, pero no se extrajo información de ningún CV. ---")

if __name__ == '__main__':
    asyncio.run(main())