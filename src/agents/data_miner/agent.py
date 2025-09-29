from dotenv import load_dotenv
from google.adk.agents import Agent
from .prompt import SYSTEM_PROMPT

# Cargar las variables de entorno desde el archivo .env en la raíz del proyecto
load_dotenv()

# --- Definición de la Herramienta (Tool) ---

def read_file_content(file_path: str) -> dict:
    """Lee y devuelve el contenido de un archivo de texto especificado."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return {"status": "success", "content": content}
    except FileNotFoundError:
        return {"status": "error", "error_message": f"El archivo no fue encontrado en la ruta {file_path}"}
    except Exception as e:
        return {"status": "error", "error_message": f"Ocurrió un error al leer el archivo: {e}"}

# --- Instanciación del Agente ---

data_miner_agent = Agent(
    name="data_miner_agent",
    model="gemini-2.5-pro",
    description="Un agente que extrae datos estructurados de archivos CV en formato Markdown.",
    instruction=SYSTEM_PROMPT,
    tools=[read_file_content]
)