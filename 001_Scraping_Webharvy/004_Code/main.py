import os
import sys
import subprocess
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()
PENDING_LINKS_DIR = os.getenv('PENDING_LINKS_DIR')
PENDING_CSV_DIR = os.getenv('PENDING_CSV_DIR')
PENDING_PHOTOS_DIR = os.getenv('PENDING_PHOTOS_DIR')

def ejecutar_proceso_completo():
    """Ejecuta todo el proceso de scraping una vez"""
    print(f"\n🚀 INICIANDO NUEVO CICLO DE PROCESAMIENTO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Ejecutar Extract_links.py para descargar y crear lotes
    print("📋 PASO 1: Extrayendo links desde BigQuery...")
    subprocess.run([sys.executable, 'Extract_links.py'], cwd=os.path.dirname(__file__))

    # Verificar si se generaron archivos .txt nuevos en Pending_Links
    pending_txts = [f for f in os.listdir(PENDING_LINKS_DIR) if f.startswith('Link_lote_') and f.endswith('.txt')]
    if not pending_txts:
        print('❌ No hay archivos .txt en Pending_Links. Finalizando ciclo.')
        return False

    print(f"✅ Se encontraron {len(pending_txts)} archivos de lotes para procesar")

    # Ejecutar automatiza_webharvy.py para procesar los lotes
    print("🤖 PASO 2: Ejecutando WebHarvy...")
    subprocess.run([sys.executable, 'automatiza_webharvy.py'], cwd=os.path.dirname(__file__))

    # Monitorear la aparición del CSV (en lugar de esperar 7 minutos fijos)
    print('⏱️  PASO 3: Monitoreando la aparición del archivo CSV...')
    csv_appeared = False
    max_wait_minutes = 10  # Máximo 10 minutos de espera
    start_time = time.time()

    while not csv_appeared and (time.time() - start_time) < (max_wait_minutes * 60):
        pending_csvs = [f for f in os.listdir(PENDING_CSV_DIR) if f.lower().endswith('.csv')]
        if pending_csvs:
            csv_appeared = True
            print(f'✅ CSV detectado: {pending_csvs[0]}')
            break
        
        elapsed = int(time.time() - start_time)
        minutes = elapsed // 60
        seconds = elapsed % 60
        print(f'⏳ Esperando CSV... Tiempo transcurrido: {minutes:02d}:{seconds:02d}', end='\r')
        time.sleep(5)  # Verificar cada 5 segundos

    if not csv_appeared:
        print(f'\n❌ No apareció ningún CSV después de {max_wait_minutes} minutos. Finalizando ciclo.')
        
        # Cerrar WebHarvy si no apareció el CSV
        import subprocess as sp
        pid_file = os.path.join(os.path.dirname(__file__), 'webharvy_pid.txt')
        if os.path.exists(pid_file):
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                sp.run(['taskkill', '/PID', str(pid), '/F'], capture_output=True, text=True, check=False)
                os.remove(pid_file)
                print(f'🔒 WebHarvy (PID: {pid}) cerrado por timeout.')
            except:
                pass
        return False

    print(f'\n✅ CSV encontrado. Verificando carpetas de imágenes...')

    # Verificar si se generaron carpetas en Pending_Photos
    pending_folders = [f for f in os.listdir(PENDING_PHOTOS_DIR) if os.path.isdir(os.path.join(PENDING_PHOTOS_DIR, f)) and f != '002_Processed_Photos']
    if not pending_folders:
        print('❌ No hay carpetas de imágenes generadas. Finalizando ciclo.')
        return False

    print(f"✅ Se encontraron {len(pending_folders)} carpetas de imágenes")

    # Ejecutar load_image_buket.py para subir imágenes y mover archivos
    print("📷 PASO 4: Procesando y subiendo imágenes...")
    subprocess.run([sys.executable, 'load_image_buket.py'], cwd=os.path.dirname(__file__))

    # Ejecutar upload_data_bd.py para subir datos de imágenes a BigQuery
    print("🗄️  PASO 5: Subiendo datos a BigQuery...")
    subprocess.run([sys.executable, 'upload_data_bd.py'], cwd=os.path.dirname(__file__))

    print("🎉 CICLO COMPLETADO EXITOSAMENTE!")
    print("=" * 80)
    return True

# BUCLE PRINCIPAL INFINITO
print("🔄 SISTEMA DE PROCESAMIENTO CONTINUO INICIADO")
print("   El sistema se ejecutará indefinidamente cada 5 minutos")
print("   Presiona Ctrl+C para detener el proceso")
print("=" * 80)

ciclo_numero = 1

try:
    while True:
        print(f"\n🔄 CICLO #{ciclo_numero}")
        
        # Ejecutar el proceso completo
        exito = ejecutar_proceso_completo()
        
        if exito:
            print(f"✅ Ciclo #{ciclo_numero} completado exitosamente")
        else:
            print(f"⚠️  Ciclo #{ciclo_numero} terminó sin procesar datos")
        
        ciclo_numero += 1
        
        # Esperar 5 minutos antes del siguiente ciclo
        print(f"\n⏰ Esperando 5 minutos antes del siguiente ciclo...")
        print(f"   Próximo ciclo: {(datetime.now() + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')}")
        
        for i in range(300):  # 300 segundos = 5 minutos
            remaining = 300 - i
            minutes = remaining // 60
            seconds = remaining % 60
            print(f"⏳ Tiempo restante: {minutes:02d}:{seconds:02d}", end='\r')
            time.sleep(1)
        
        print("\n" + "=" * 80)

except KeyboardInterrupt:
    print(f"\n\n🛑 PROCESO DETENIDO POR EL USUARIO")
    print(f"   Total de ciclos ejecutados: {ciclo_numero - 1}")
    print("   Sistema terminado correctamente.")
    input('Presiona Enter para salir...')
