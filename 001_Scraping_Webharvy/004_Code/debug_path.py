import os
from dotenv import load_dotenv

load_dotenv()
PROJECT_PATH = os.getenv('PROJECT_PATH')
print(f"PROJECT_PATH desde .env: '{PROJECT_PATH}'")
print(f"Ruta absoluta: '{os.path.abspath(PROJECT_PATH)}'")
print(f"¿Existe el archivo?: {os.path.exists(PROJECT_PATH)}")