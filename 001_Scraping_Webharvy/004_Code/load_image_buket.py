import os
import time
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
import shutil

# Cargar variables de entorno desde .env
load_dotenv()

CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'fb_images_cloud')
PENDING_CSV_DIR = os.getenv('PENDING_CSV_DIR')
PROCESSED_CSV_DIR = os.getenv('PROCESSED_CSV_DIR')
PENDING_PHOTOS_DIR = os.getenv('PENDING_PHOTOS_DIR')
PROCESSED_PHOTOS_DIR = os.getenv('PROCESSED_PHOTOS_DIR')

# Monitorear el CSV en Pending_CSV
csv_files = [f for f in os.listdir(PENDING_CSV_DIR) if f.lower().endswith('.csv')]
if not csv_files:
    print('No hay archivos CSV en Pending_CSV. Proceso terminado.')
    exit(0)
csv_file = csv_files[0]
csv_path = os.path.join(PENDING_CSV_DIR, csv_file)

print(f'Monitoreando el archivo CSV: {csv_file}')

# Esperar hasta que el CSV no se modifique por 6 minutos
def wait_for_csv(csv_path, wait_minutes=6, check_interval=60):
    print(f'Monitoreando estabilidad del CSV: {os.path.basename(csv_path)}')
    while True:
        last_mod = os.path.getmtime(csv_path)
        now = time.time()
        print(f'Última modificación: {datetime.fromtimestamp(last_mod)}')
        time.sleep(check_interval)
        new_mod = os.path.getmtime(csv_path)
        if new_mod == last_mod and (now - last_mod) > wait_minutes * 60:
            print(f'✅ El archivo CSV no se ha modificado en {wait_minutes} minutos. Continuando...')
            break
        else:
            print(f'ℹ️  CSV aún se está modificando, esperando...')

wait_for_csv(csv_path)

# Leer y procesar el CSV para renombrar imágenes
import csv
import re

print("📋 PROCESANDO CSV PARA RENOMBRADO...")
csv_data = []
image_columns = ['profile_image', 'cover_image', 'post_image1', 'post_image2']

# Leer el CSV
with open(csv_path, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    csv_data = list(reader)

print(f"📊 Registros encontrados en CSV: {len(csv_data)}")

# Diagnosticar estructura del CSV
if csv_data:
    print(f"📋 Columnas encontradas en CSV: {list(csv_data[0].keys())}")
    print(f"📋 Primera fila de ejemplo: {csv_data[0]}")
else:
    print("❌ El CSV está vacío")
    exit(1)

# Función para limpiar nombre de empresa
def clean_name(name):
    # Remover caracteres especiales y espacios, reemplazar con guiones bajos
    clean = re.sub(r'[^\w\s-]', '', name)
    clean = re.sub(r'\s+', '_', clean.strip())
    return clean

# Inicializar el cliente de Google Cloud Storage
storage_client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
bucket = storage_client.bucket(BUCKET_NAME)

# Recolectar y renombrar TODAS las imágenes basado en el CSV
print("🔍 RECOLECTANDO Y RENOMBRANDO IMÁGENES...")
pending_items = os.listdir(PENDING_PHOTOS_DIR)
folders = [f for f in pending_items if os.path.isdir(os.path.join(PENDING_PHOTOS_DIR, f)) and f != '002_Processed_Photos']

print(f"📁 Carpetas encontradas: {folders}")

# Lista para almacenar las imágenes renombradas
renamed_images = []
updated_csv_data = []

# PRIMERO: Obtener los links e id_scraping antes del procesamiento de imágenes
print(f"\n🔗 OBTENIENDO LINKS E ID_SCRAPING...")

# Buscar el último archivo procesado en 002_Processed_Links
PROCESSED_LINKS_DIR = os.getenv('PROCESSED_LINKS_DIR')
processed_files = [f for f in os.listdir(PROCESSED_LINKS_DIR) if f.startswith('Link_lote_') and f.endswith('.txt')]
links_data = []

if processed_files:
    # Obtener el último archivo procesado (por fecha de modificación)
    processed_files_with_time = []
    for f in processed_files:
        file_path = os.path.join(PROCESSED_LINKS_DIR, f)
        mod_time = os.path.getmtime(file_path)
        processed_files_with_time.append((f, mod_time))
    
    # Ordenar por tiempo de modificación (más reciente primero)
    processed_files_with_time.sort(key=lambda x: x[1], reverse=True)
    latest_file = processed_files_with_time[0][0]
    latest_file_path = os.path.join(PROCESSED_LINKS_DIR, latest_file)
    
    print(f"📄 Archivo de links más reciente: {latest_file}")
    
    # Leer los links y id_scraping del archivo
    with open(latest_file_path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip()]
        # Separar links e id_scraping
        for line in lines:
            if '|' in line:
                link, id_scraping = line.split('|', 1)
                links_data.append((link.strip(), id_scraping.strip()))
            else:
                # Si no hay separador, usar línea como link y id_scraping vacío
                links_data.append((line.strip(), ''))
    
    print(f"🔗 Links encontrados: {len(links_data)}")
    print(f"📊 Filas en CSV: {len(csv_data)}")
else:
    print("❌ No se encontraron archivos de links procesados")

# Detectar columna de nombre (puede ser 'name', 'Name', o similar)
name_column = None
possible_name_columns = ['name', 'Name', 'NAME', 'empresa', 'company', 'negocio']

# Limpiar nombres de columnas (remover BOM y comillas extra)
cleaned_columns = {}
for col in csv_data[0].keys():
    clean_col = col.replace('\ufeff', '').replace('"', '').strip()
    cleaned_columns[col] = clean_col

# Buscar columna de nombres en versiones limpias
for original_col, clean_col in cleaned_columns.items():
    if clean_col in possible_name_columns:
        name_column = original_col
        break

if not name_column:
    # Si no encuentra columna de nombre, usar la primera columna
    name_column = list(csv_data[0].keys())[0]
    print(f"⚠️  No se encontró columna 'name', usando: {cleaned_columns[name_column]}")

print(f"📋 Usando columna para nombres: {cleaned_columns[name_column]}")

for row_idx, row in enumerate(csv_data):
    company_name = clean_name(row[name_column])
    
    # Obtener id_scraping desde los links_data
    row_id_scraping = 'unknown'
    if row_idx < len(links_data):
        link, id_scraping = links_data[row_idx]
        row_id_scraping = id_scraping
        # Agregar Link e id_scraping al row
        row['Link'] = link
        row['id_scraping'] = id_scraping
        print(f"  ✅ Asociado: {link} | ID: {id_scraping}")
    else:
        row['Link'] = ''
        row['id_scraping'] = ''
        print(f"  ⚠️  Sin link/ID asociado")
    
    print(f"\n--- Procesando: {row[name_column]} (ID: {row_id_scraping}) → {company_name} ---")
    
    updated_row = row.copy()
    
    for img_column in image_columns:
        if img_column in row and row[img_column]:
            original_filename = row[img_column]
            
            # Buscar el archivo en la carpeta correspondiente
            folder_path = os.path.join(PENDING_PHOTOS_DIR, img_column)
            if os.path.exists(folder_path):
                # Buscar archivo que coincida (considerando caracteres especiales)
                found_file = None
                try:
                    files_in_folder = os.listdir(folder_path)
                    print(f"    📁 Archivos en {img_column}: {files_in_folder}")
                    
                    for file in files_in_folder:
                        # Método 1: Coincidencia exacta
                        if file == original_filename:
                            found_file = os.path.join(folder_path, file)
                            print(f"    ✅ Coincidencia exacta: {file}")
                            break
                        # Método 2: Por prefijo (antes del &)
                        elif file.startswith(original_filename.split('&')[0]):
                            found_file = os.path.join(folder_path, file)
                            print(f"    ✅ Coincidencia por prefijo: {file}")
                            break
                        # Método 3: Buscar por similitud parcial (primeros 10 caracteres)
                        elif len(original_filename) > 10 and file.startswith(original_filename[:10]):
                            found_file = os.path.join(folder_path, file)
                            print(f"    ✅ Coincidencia parcial: {file}")
                            break
                    
                    if not found_file:
                        print(f"    ❌ No se encontró coincidencia para: {original_filename}")
                        print(f"    🔍 Buscaba: {original_filename}")
                        print(f"    🔍 Prefijo: {original_filename.split('&')[0]}")
                        
                except Exception as e:
                    print(f"    ❌ Error listando {img_column}: {e}")
                    continue
                
                if found_file:
                    print(f"    🔍 Ruta construida: {found_file}")
                    print(f"    🔍 ¿Existe la ruta?: {os.path.exists(found_file)}")
                    
                    # Intentar diferentes métodos para acceder al archivo
                    access_methods = [
                        ("Ruta normal", found_file),
                        ("Ruta absoluta", os.path.abspath(found_file)),
                        ("Ruta raw", repr(found_file)),
                    ]
                    
                    successful_path = None
                    for method_name, test_path in access_methods:
                        if os.path.exists(test_path):
                            successful_path = test_path
                            print(f"    ✅ {method_name} funciona: {test_path}")
                            break
                        else:
                            print(f"    ❌ {method_name} falla: {test_path}")
                    
                    if successful_path:
                        # Crear nuevo nombre usando id_scraping en lugar de company_name
                        file_extension = '.jpg'  # Asumir .jpg
                        new_filename = f"{row_id_scraping}_{img_column}{file_extension}"
                        new_path = os.path.join(folder_path, new_filename)
                        
                        try:
                            # Renombrar archivo físico
                            os.rename(successful_path, new_path)
                            renamed_images.append(new_path)
                            
                            # Actualizar CSV con URL completa de Google Cloud Storage
                            gcs_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{new_filename}"
                            updated_row[img_column] = gcs_url
                            print(f"  ✅ {img_column}: {original_filename} → {gcs_url}")
                            
                        except Exception as e:
                            print(f"  ❌ Error renombrando {img_column}: {e}")
                            print(f"      Desde: {successful_path}")
                            print(f"      Hacia: {new_path}")
                            updated_row[img_column] = original_filename
                    else:
                        print(f"  ⚠️  {img_column}: Ningún método de acceso funcionó")
                        updated_row[img_column] = original_filename
                else:
                    print(f"  ⚠️  {img_column}: Archivo no encontrado en listado")
                    updated_row[img_column] = original_filename
    
    updated_csv_data.append(updated_row)

print(f"\n📊 Total de imágenes renombradas: {len(renamed_images)}")

# Agregar columna created_at al CSV
print(f"\n� AGREGANDO COLUMNA CREATED_AT...")
current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"� Timestamp actual: {current_timestamp}")

# Agregar created_at a cada fila
for row in updated_csv_data:
    row['created_at'] = current_timestamp

# Generar timestamp para archivos
now_str = datetime.now().strftime('%Y%m%d_%H%M%S')

# Guardar CSV actualizado (solo las rutas de imágenes cambian, no el name)
updated_csv_path = csv_path.replace('.csv', f'_{now_str}.csv')

# Crear headers limpios (sin BOM ni comillas extra) + agregar Link, id_scraping y created_at
clean_headers = []
for col in csv_data[0].keys():
    clean_col = col.replace('\ufeff', '').replace('"', '').strip()
    clean_headers.append(clean_col)

# Agregar columnas nuevas al final (sin duplicar)
clean_headers.extend(['Link', 'id_scraping', 'created_at'])

# Crear datos limpios con headers correctos - EVITAR DUPLICADOS
clean_csv_data = []
for row in updated_csv_data:
    clean_row = {}
    # Mapear solo las columnas originales del CSV
    for original_col in csv_data[0].keys():
        clean_col = original_col.replace('\ufeff', '').replace('"', '').strip()
        clean_row[clean_col] = row[original_col]
    
    # Agregar las nuevas columnas sin duplicar
    clean_row['Link'] = row.get('Link', '')
    clean_row['id_scraping'] = row.get('id_scraping', '')
    clean_row['created_at'] = row.get('created_at', '')
    
    clean_csv_data.append(clean_row)

# Escribir CSV limpio sin BOM ni comillas innecesarias
with open(updated_csv_path, 'w', newline='', encoding='utf-8') as f:
    if clean_csv_data:
        writer = csv.DictWriter(f, fieldnames=clean_headers, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(clean_csv_data)

print(f"📄 CSV actualizado guardado: {os.path.basename(updated_csv_path)}")

# Subir TODAS las imágenes renombradas al bucket
print(f"\n🚀 SUBIENDO {len(renamed_images)} IMÁGENES AL BUCKET...")
for i, local_path in enumerate(renamed_images, 1):
    blob_path = os.path.basename(local_path)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    print(f"  ✅ ({i}/{len(renamed_images)}) {blob_path}")

print(f"\n🎉 TODAS LAS IMÁGENES SUBIDAS EXITOSAMENTE!")

# Mover TODAS las carpetas a procesados
print(f"\n📦 MOVIENDO CARPETAS A PROCESADOS...")
processed_folder = os.path.join(PENDING_PHOTOS_DIR, '002_Processed_Photos', now_str)
os.makedirs(processed_folder, exist_ok=True)

for folder in folders:
    src = os.path.join(PENDING_PHOTOS_DIR, folder)
    new_name = f"{folder}_{now_str}"
    dst = os.path.join(processed_folder, new_name)
    shutil.move(src, dst)
    print(f"  ✅ {folder} → {new_name}")

print(f"🎉 CARPETAS MOVIDAS A: {processed_folder}")

# Renombrar y mover el CSV procesado (solo uno con rutas actualizadas)
# El CSV original se elimina porque ya tenemos el actualizado
os.remove(csv_path)
shutil.move(updated_csv_path, os.path.join(PROCESSED_CSV_DIR, os.path.basename(updated_csv_path)))
print(f"📄 CSV procesado movido: {os.path.basename(updated_csv_path)}")

# Cerrar WebHarvy si existe el archivo PID
pid_file = os.path.join(os.path.dirname(__file__), 'webharvy_pid.txt')
if os.path.exists(pid_file):
    try:
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        
        # Intentar cerrar WebHarvy usando taskkill (más confiable en Windows)
        import subprocess
        subprocess.run(['taskkill', '/PID', str(pid), '/F'], 
                      capture_output=True, text=True, check=False)
        print(f"WebHarvy (PID: {pid}) cerrado exitosamente")
        
        # Eliminar el archivo PID
        os.remove(pid_file)
        
    except Exception as e:
        print(f"No se pudo cerrar WebHarvy: {e}")
else:
    print("No se encontró archivo PID de WebHarvy")

print("Proceso completado exitosamente.")
