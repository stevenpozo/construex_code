import os
import csv
import glob
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
PROCESSED_CSV_DIR = os.getenv('PROCESSED_CSV_DIR')

if not SERVICE_ACCOUNT_FILE:
    raise ValueError("La variable GOOGLE_APPLICATION_CREDENTIALS no está definida en el archivo .env")
if not PROCESSED_CSV_DIR:
    raise ValueError("La variable PROCESSED_CSV_DIR no está definida en el archivo .env")

# Crear credenciales y cliente de BigQuery
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Tabla destino
TABLE_ID = "web-scraping-468121.web_scraping_raw_data.mx_web_scraping_images_cleaned"

print("🚀 INICIANDO PROCESO DE SUBIDA DE IMÁGENES A BIGQUERY...")

# Buscar el CSV más reciente en 002_Processed_CSV
csv_files = glob.glob(os.path.join(PROCESSED_CSV_DIR, "*.csv"))
if not csv_files:
    print("❌ No se encontraron archivos CSV en 002_Processed_CSV")
    exit(1)

# Obtener el archivo más reciente por fecha de modificación
latest_csv = max(csv_files, key=os.path.getmtime)
print(f"📄 CSV más reciente encontrado: {os.path.basename(latest_csv)}")

# Leer el CSV
csv_data = []
with open(latest_csv, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    csv_data = list(reader)

print(f"📊 Registros encontrados en CSV: {len(csv_data)}")

# Obtener los id_scraping únicos del CSV para consultar países
id_scraping_list = []
for row in csv_data:
    if row.get('id_scraping') and row['id_scraping'].strip():
        try:
            id_scraping = int(row['id_scraping'])
            if id_scraping not in id_scraping_list:
                id_scraping_list.append(id_scraping)
        except ValueError:
            print(f"⚠️  id_scraping inválido: {row['id_scraping']}")

print(f"🔍 IDs únicos para consultar países: {len(id_scraping_list)}")

# Consulta para obtener países por id_scraping
if id_scraping_list:
    id_scraping_str = ','.join(map(str, id_scraping_list))
    
    QUERY_COUNTRIES = f'''
    SELECT
      ws.id_scraping, ws.Pais
    FROM
      `web-scraping-468121.web_scraping_raw_data.mx_web_scraping_raw_update_copy` ws
    WHERE
      ws.stand_id_construex IS NULL
      AND ws.Email != 'No Disponible'
      AND LOWER(ws.Link) LIKE '%facebook%'
      AND ws.id_scraping IN ({id_scraping_str});
    '''
    
    print("🌍 Consultando países desde BigQuery...")
    query_job = client.query(QUERY_COUNTRIES)
    countries_result = query_job.result()
    
    # Crear diccionario id_scraping -> país
    countries_dict = {}
    for row in countries_result:
        # Formatear país: primera letra mayúscula
        country = row['Pais'].strip().capitalize() if row['Pais'] else ''
        countries_dict[row['id_scraping']] = country
    
    print(f"📍 Países obtenidos: {len(countries_dict)}")
else:
    countries_dict = {}
    print("⚠️  No hay IDs válidos para consultar países")

# Procesar cada fila del CSV y generar registros para BigQuery
records_to_insert = []
image_columns = ['profile_image', 'cover_image', 'post_image1', 'post_image2']

print("\n📋 PROCESANDO REGISTROS DEL CSV...")

for i, row in enumerate(csv_data, 1):
    try:
        id_scraping = int(row.get('id_scraping', 0))
        country = countries_dict.get(id_scraping, '')
        
        if not country:
            print(f"⚠️  Fila {i}: No se encontró país para id_scraping {id_scraping}")
            country = 'Unknown'
        
        print(f"\n--- Fila {i}: ID {id_scraping} | País: {country} ---")
        
        # Procesar cada tipo de imagen
        for image_column in image_columns:
            img_url = row.get(image_column, '').strip()
            
            if img_url and img_url.startswith('https://'):
                # Determinar el tipo de imagen
                if image_column == 'post_image1' or image_column == 'post_image2':
                    image_type = 'post_image'
                else:
                    image_type = image_column  # profile_image, cover_image
                
                # Obtener created_at del CSV y convertir al formato correcto para BigQuery
                created_at_str = row.get('created_at', '')
                created_at_formatted = None
                
                if created_at_str:
                    try:
                        # Convertir string a datetime y luego a formato ISO para BigQuery
                        from datetime import datetime
                        dt = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
                        created_at_formatted = dt.isoformat()
                    except ValueError:
                        print(f"  ⚠️  Formato de fecha inválido: {created_at_str}")
                        created_at_formatted = None
                
                # Crear registro para BigQuery
                record = {
                    'id_scraping': id_scraping,
                    'country': country,
                    'img_path': img_url,
                    'image_type': image_type,
                    'created_at': created_at_formatted
                }
                
                records_to_insert.append(record)
                print(f"  ✅ {image_type}: {img_url} | Created: {created_at_str}")
            else:
                print(f"  ⚠️  {image_column}: Sin URL válida")
                
    except ValueError as e:
        print(f"❌ Error procesando fila {i}: {e}")
        continue

print(f"\n📊 TOTAL DE REGISTROS A INSERTAR: {len(records_to_insert)}")

if records_to_insert:
    print(f"\n🚀 INSERTANDO {len(records_to_insert)} REGISTROS EN BIGQUERY...")
    
    # Configurar job de inserción
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Agregar a tabla existente
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    
    try:
        # Insertar registros
        job = client.load_table_from_json(
            records_to_insert,
            TABLE_ID,
            job_config=job_config
        )
        
        # Esperar a que termine el job
        job.result()
        
        print(f"✅ INSERCIÓN EXITOSA!")
        print(f"   📊 Registros insertados: {len(records_to_insert)}")
        print(f"   🗂️  Tabla: {TABLE_ID}")
        print(f"   📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Mostrar estadísticas por tipo de imagen
        type_stats = {}
        for record in records_to_insert:
            img_type = record['image_type']
            type_stats[img_type] = type_stats.get(img_type, 0) + 1
        
        print(f"\n📈 ESTADÍSTICAS POR TIPO DE IMAGEN:")
        for img_type, count in type_stats.items():
            print(f"   📷 {img_type}: {count} registros")
            
    except Exception as e:
        print(f"❌ ERROR EN LA INSERCIÓN: {e}")
        exit(1)
        
else:
    print("⚠️  No hay registros para insertar")

print(f"\n🎉 PROCESO COMPLETADO EXITOSAMENTE!")
print(f"CSV procesado: {os.path.basename(latest_csv)}")