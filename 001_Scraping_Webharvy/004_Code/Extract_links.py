import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import re

# Cargar variables de entorno desde .env
load_dotenv()

# Obtener ruta de credenciales y carpeta de salida desde .env
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
OUTPUT_DIR = os.getenv('PENDING_LINKS_DIR')
if not SERVICE_ACCOUNT_FILE:
    raise ValueError("La variable GOOGLE_APPLICATION_CREDENTIALS no está definida en el archivo .env")
if not OUTPUT_DIR:
    raise ValueError("La variable PENDING_LINKS_DIR no está definida en el archivo .env")

# Crear credenciales y cliente de BigQuery
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Consulta para obtener los links de Facebook no procesados
QUERY_SELECT = '''
SELECT 
  Link, id_scraping 
FROM 
  `web-scraping-468121.web_scraping_raw_data.mx_web_scraping_raw_update_copy_new`
WHERE
  processed = false;
'''

# Consulta para actualizar los links como procesados
QUERY_UPDATE = '''
UPDATE
  `web-scraping-468121.web_scraping_raw_data.mx_web_scraping_raw_update_copy_new`
SET
  processed = true
WHERE
  processed = false;
'''

# Ejecutar la consulta SELECT
query_job = client.query(QUERY_SELECT)
results = [(row['Link'], row['id_scraping']) for row in query_job.result()]

# Crear carpeta de salida si no existe
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Buscar el último número de lote existente
existing_files = [f for f in os.listdir(OUTPUT_DIR) if re.match(r'Link_lote_(\\d+).txt', f)]
last_lote = 0
for f in existing_files:
    match = re.match(r'Link_lote_(\\d+).txt', f)
    if match:
        num = int(match.group(1))
        if num > last_lote:
            last_lote = num

# Guardar los links en lotes de máximo 8 por archivo
lote = last_lote + 1
for i in range(0, len(results), 8):
    links_lote = results[i:i+8]
    file_path = os.path.join(OUTPUT_DIR, f'Link_lote_{lote}.txt')
    with open(file_path, 'w', encoding='utf-8') as f:
        for link, id_scraping in links_lote:
            f.write(f"{link}|{id_scraping}\n")
    lote += 1

print(f"Se generaron {lote - last_lote - 1} archivos de lotes en {OUTPUT_DIR}.")

# Ejecutar la consulta UPDATE para marcar como procesados
update_job = client.query(QUERY_UPDATE)
update_job.result()
print("Links marcados como procesados en BigQuery.")
