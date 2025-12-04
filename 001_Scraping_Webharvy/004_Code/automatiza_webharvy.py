import os
import subprocess
import glob
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
import shutil
import psutil

# Cargar variables de entorno desde .env
load_dotenv()

WEBHARVY_PATH = os.getenv('WEBHARVY_PATH')
PROJECT_PATH = os.getenv('PROJECT_PATH')
PENDING_LINKS_DIR = os.getenv('PENDING_LINKS_DIR')
PROCESSED_LINKS_DIR = os.getenv('PROCESSED_LINKS_DIR')
PENDING_CSV_DIR = os.getenv('PENDING_CSV_DIR')

if not WEBHARVY_PATH or not PROJECT_PATH or not PENDING_LINKS_DIR or not PROCESSED_LINKS_DIR:
    print('Faltan variables en el archivo .env. Proceso terminado.')
    exit(1)

# ===========================================
# LIMPIEZA INICIAL - VERIFICAR CSV RESIDUAL Y CERRAR WEBHARVY
# ===========================================
print('🧹 VERIFICANDO ESTADO INICIAL DEL SISTEMA...')

# 1. Verificar y eliminar CSV residual en PENDING_CSV
if PENDING_CSV_DIR and os.path.exists(PENDING_CSV_DIR):
    csv_files = [f for f in os.listdir(PENDING_CSV_DIR) if f.lower().endswith('.csv')]
    if csv_files:
        print(f'⚠️  DETECTADO CSV RESIDUAL DE PROCESO ANTERIOR: {csv_files}')
        for csv_file in csv_files:
            csv_path = os.path.join(PENDING_CSV_DIR, csv_file)
            try:
                os.remove(csv_path)
                print(f'🗑️  CSV residual eliminado: {csv_file}')
            except Exception as e:
                print(f'❌ Error al eliminar CSV residual {csv_file}: {e}')
    else:
        print('✅ No hay CSV residual en PENDING_CSV')
else:
    print('⚠️  PENDING_CSV_DIR no está definido en .env')

# 2. Cerrar cualquier instancia de WebHarvy que pueda estar ejecutándose
def close_webharvy_processes():
    """Cierra todas las instancias de WebHarvy que estén ejecutándose"""
    closed_processes = 0
    
    # Buscar procesos por nombre
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.info['name'] and 'webharvy' in proc.info['name'].lower():
                print(f'🔄 Cerrando proceso WebHarvy (PID: {proc.info["pid"]})')
                proc.terminate()
                closed_processes += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    # También buscar por archivo PID si existe
    pid_file = os.path.join(os.path.dirname(__file__), 'webharvy_pid.txt')
    if os.path.exists(pid_file):
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
            
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                proc.terminate()
                print(f'🔄 Proceso WebHarvy cerrado desde PID file (PID: {pid})')
                closed_processes += 1
            
            # Eliminar el archivo PID
            os.remove(pid_file)
            print('🗑️  Archivo PID eliminado')
            
        except Exception as e:
            print(f'⚠️  Error procesando archivo PID: {e}')
    
    return closed_processes

closed_count = close_webharvy_processes()
if closed_count > 0:
    print(f'✅ {closed_count} proceso(s) de WebHarvy cerrado(s)')
else:
    print('✅ No hay procesos de WebHarvy ejecutándose')

print('🎯 LIMPIEZA INICIAL COMPLETADA. Iniciando nuevo proceso...\n')

# ===========================================
# CONTINUAR CON EL PROCESO NORMAL
# ===========================================

# Buscar el primer archivo .txt disponible
txt_files = sorted(glob.glob(os.path.join(PENDING_LINKS_DIR, 'Link_lote_*.txt')), key=lambda x: int(os.path.splitext(os.path.basename(x))[0].split('_')[-1]))
if not txt_files:
    print('No hay archivos .txt disponibles en Pending_Links. Proceso terminado.')
    exit(0)

first_txt = txt_files[0]
with open(first_txt, 'r', encoding='utf-8') as f:
    lines = [line.strip() for line in f if line.strip()]
    # Extraer solo los links (antes del |), ignorar los id_scraping
    links = [line.split('|')[0] for line in lines if '|' in line]

if not links:
    print('El archivo .txt está vacío. Proceso terminado.')
    exit(0)

# Modificar el XML con los nuevos links
ET.register_namespace('', "http://www.w3.org/2001/XMLSchema")
tree = ET.parse(PROJECT_PATH)
root = tree.getroot()

# Reemplazar el primer link en <StartURL>
start_url_elem = root.find('.//StartURL/url')
if start_url_elem is not None:
    start_url_elem.text = links[0]

# Reemplazar los siguientes links en <URLList>
url_list_elem = root.find('.//URLList')
if url_list_elem is not None:
    # Limpiar los elementos actuales
    for url_data in list(url_list_elem):
        url_list_elem.remove(url_data)
    # Agregar los links
    for i, link in enumerate(links):
        url_data = ET.Element('URLDATA')
        if i > 0:
            name_elem = ET.SubElement(url_data, 'name')
            name_elem.text = 'URL'
        url_elem = ET.SubElement(url_data, 'url')
        url_elem.text = link
        url_list_elem.append(url_data)

# Guardar el XML modificado
project_path_bak = PROJECT_PATH + '.bak'
os.replace(PROJECT_PATH, project_path_bak)
tree.write(PROJECT_PATH, encoding='utf-16', xml_declaration=True)

print(f'Links reemplazados en el proyecto XML: {links}')

# Mover el txt procesado a 002_Processed_Links
processed_txt_path = os.path.join(PROCESSED_LINKS_DIR, os.path.basename(first_txt))
shutil.move(first_txt, processed_txt_path)
print(f'Archivo {os.path.basename(first_txt)} movido a {PROCESSED_LINKS_DIR}')

# Abrir WebHarvy con el proyecto
process = subprocess.Popen([WEBHARVY_PATH, PROJECT_PATH])
print(f'WebHarvy abierto con el proyecto. PID: {process.pid}')

# Guardar PID en archivo para posterior cierre
pid_file = os.path.join(os.path.dirname(__file__), 'webharvy_pid.txt')
with open(pid_file, 'w') as f:
    f.write(str(process.pid))
