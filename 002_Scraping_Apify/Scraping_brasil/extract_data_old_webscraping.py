import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import json
import re
from difflib import SequenceMatcher
from pathlib import Path

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration_process.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

@dataclass
class CompanyData:
    """Estructura para datos de empresa"""
    link: str
    id_scraping: int
    pais: str
    address: str
    category: str
    email: str
    intro: str
    phone: str
    title: str

@dataclass
class ImageData:
    """Estructura para datos de imagen"""
    id_scraping: int
    country: str
    img_path: str
    image_type: str
    id_photo_cleaned: int

@dataclass
class ProcessStats:
    """Estadísticas del proceso"""
    companies_found_gcs: int = 0
    companies_found_table1: int = 0
    companies_migrated_table2: int = 0
    companies_with_images: int = 0
    companies_without_images: int = 0
    total_images_processed: int = 0
    execution_time_seconds: float = 0.0
    errors_count: int = 0

class DataMigrationProcessor:
    """Procesador principal para la migración de datos"""
    
    def __init__(self):
        self.project_id = os.getenv('BIGQUERY_STORAGE_ID')
        self.service_account_path = os.getenv('BIGQUERY_STORAGE_SERVICE')
        self.bucket_drive_name = os.getenv('BUCKET_DRIVE_NAME')
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.tabla1 = os.getenv('TABLA_1')
        self.tabla2 = os.getenv('TABLA_2')
        self.tabla3 = os.getenv('TABLA_3')
        self.folder_brasil1 = os.getenv('FOLDER_BRASIL1')  # Nueva variable
        
        # Configurar timezone Ecuador
        self.ecuador_tz = pytz.timezone('America/Guayaquil')
        
        # Inicializar clientes
        self.storage_client = storage.Client.from_service_account_json(self.service_account_path)
        self.bq_client = bigquery.Client.from_service_account_json(self.service_account_path)
        
        # Estadísticas
        self.stats = ProcessStats()
        
        # Contador para ID único de fotos
        self.photo_id_counter = int(time.time() * 1000)  # Usar timestamp como base
        
        # Verificar configuración del bucket destino
        self.check_bucket_public_access()
        
        logger.info("[OK] Inicialización completada")
        logger.info(f"[INFO] Proyecto: {self.project_id}")
        logger.info(f"[INFO] Bucket Drive: {self.bucket_drive_name}")
        logger.info(f"[INFO] Bucket Destino: {self.bucket_name}")
        logger.info(f"[INFO] Carpeta Brasil: {self.folder_brasil1}")

    def check_bucket_public_access(self):
        """Verificar si el bucket destino está configurado para acceso público"""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            
            # Verificar si tiene Uniform Bucket-Level Access habilitado
            bucket.reload()
            
            if bucket.iam_configuration.uniform_bucket_level_access_enabled:
                logger.info(f"[INFO] Bucket {self.bucket_name} tiene Uniform Bucket-Level Access habilitado")
                logger.info(f"[INFO] Las imágenes se subirán sin configurar ACLs individuales")
                logger.info(f"[INFO] Asegúrate de que el bucket tenga política IAM pública configurada")
            else:
                logger.info(f"[INFO] Bucket {self.bucket_name} permite ACLs a nivel de objeto")
                
        except Exception as e:
            logger.warning(f"[WARNING] No se pudo verificar configuración del bucket: {str(e)}")
            logger.info(f"[INFO] Continuando con el proceso...")

    def normalize_company_name(self, name: str) -> str:
        """Normalizar nombres de empresas para comparación"""
        if not name:
            return ""
        
        # Remover caracteres especiales y convertir a minúsculas
        normalized = re.sub(r'[^a-zA-Z0-9\s]', '', name.lower())
        # Remover espacios extra
        normalized = ' '.join(normalized.split())
        return normalized

    def calculate_similarity(self, name1: str, name2: str) -> float:
        """Calcular similitud entre dos nombres de empresas"""
        norm1 = self.normalize_company_name(name1)
        norm2 = self.normalize_company_name(name2)
        return SequenceMatcher(None, norm1, norm2).ratio()

    def get_companies_from_gcs(self) -> List[str]:
        """Obtener nombres de empresas desde Google Cloud Storage"""
        logger.info("[BUSCAR] Buscando empresas en Google Cloud Storage...")
        
        try:
            bucket = self.storage_client.bucket(self.bucket_drive_name)
            brasil_path = f"Webscraping/Paises/Old Web Scraping/{self.folder_brasil1}/Posts/"
            
            companies = []
            blobs = bucket.list_blobs(prefix=brasil_path, delimiter='/')
            
            for page in blobs.pages:
                for prefix in page.prefixes:
                    # Extraer nombre de empresa del path
                    company_name = prefix.replace(brasil_path, '').rstrip('/')
                    if company_name and company_name != brasil_path:
                        companies.append(company_name)
            
            self.stats.companies_found_gcs = len(companies)
            logger.info(f"[STATS] Total empresas encontradas en GCS: {self.stats.companies_found_gcs}")
            
            return companies
            
        except Exception as e:
            logger.error(f"[ERROR] Error obteniendo empresas de GCS: {str(e)}")
            self.stats.errors_count += 1
            return []

    def get_companies_from_table1(self) -> List[CompanyData]:
        """Obtener datos de empresas desde la Tabla 1"""
        logger.info("[BUSCAR] Obteniendo empresas de la Tabla 1...")
        
        # Primero verificar qué países hay disponibles
        debug_query = f"""
        SELECT DISTINCT Pais, COUNT(*) as count 
        FROM `{self.tabla1}` 
        GROUP BY Pais 
        ORDER BY count DESC
        """
        
        try:
            debug_job = self.bq_client.query(debug_query)
            debug_results = debug_job.result()
            
            logger.info("[DEBUG] Países disponibles en Tabla 1:")
            for row in debug_results:
                logger.info(f"[DEBUG] - {row.Pais}: {row.count} empresas")
        except Exception as e:
            logger.error(f"[ERROR] Error en debug de países: {str(e)}")
        
        # Query principal con búsqueda más flexible y limitando campos
        query = f"""
        SELECT 
            Link, 
            id_scraping, 
            Pais, 
            Direccion as Address, 
            Categoria as Category, 
            Email, 
            Descripcion as Intro, 
            Telefono as Phone, 
            Nombre as Title 
        FROM `{self.tabla1}`
        WHERE (
            LOWER(Pais) = 'brasil' 
            OR Pais IS NULL 
            OR TRIM(Pais) = ''
        )
        AND Nombre IS NOT NULL
        AND Nombre != ''
        ORDER BY id_scraping
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            companies = []
            for row in results:
                company = CompanyData(
                    link=row.Link or '',
                    id_scraping=row.id_scraping,
                    pais=row.Pais or '',
                    address=row.Address or '',
                    category=row.Category or '',
                    email=row.Email or '',
                    intro=row.Intro or '',
                    phone=row.Phone or '',
                    title=row.Title or ''
                )
                companies.append(company)
            
            self.stats.companies_found_table1 = len(companies)
            logger.info(f"[STATS] Total empresas encontradas en Tabla 1: {self.stats.companies_found_table1}")
            
            return companies
            
        except Exception as e:
            logger.error(f"[ERROR] Error obteniendo empresas de Tabla 1: {str(e)}")
            self.stats.errors_count += 1
            return []

    def match_gcs_batch(self, gcs_batch: List[str], normalized_index: Dict, threshold: float, batch_id: int) -> List[CompanyData]:
        """Procesar un lote de empresas GCS para encontrar matches"""
        batch_matches = []
        
        for gcs_company in gcs_batch:
            gcs_normalized = self.normalize_company_name(gcs_company)
            best_match = None
            best_similarity = 0.0
            
            # Primero buscar coincidencia exacta
            if gcs_normalized in normalized_index:
                best_match = normalized_index[gcs_normalized][0]
                best_similarity = 1.0
            else:
                # Buscar en nombres que contengan palabras clave
                gcs_words = set(gcs_normalized.split())
                
                for norm_name, companies in normalized_index.items():
                    table_words = set(norm_name.split())
                    
                    # Calcular intersección de palabras
                    common_words = gcs_words.intersection(table_words)
                    if len(common_words) >= min(2, len(gcs_words)):  # Al menos 2 palabras en común
                        # Solo entonces calcular similitud exacta
                        similarity = self.calculate_similarity(gcs_company, companies[0].title)
                        
                        if similarity > best_similarity and similarity >= threshold:
                            best_similarity = similarity
                            best_match = companies[0]
            
            if best_match:
                batch_matches.append(best_match)
        
        logger.info(f"[BATCH-{batch_id}] Procesadas {len(gcs_batch)} empresas, {len(batch_matches)} matches encontrados")
        return batch_matches

    def match_companies(self, gcs_companies: List[str], table1_companies: List[CompanyData]) -> List[CompanyData]:
        """Comparar y hacer match de empresas entre GCS y Tabla 1 con threading"""
        logger.info("[MATCH] Comparando empresas entre GCS y Tabla 1...")
        logger.info(f"[STATS] Total GCS: {len(gcs_companies)}, Total Tabla1: {len(table1_companies)}")
        
        # Crear índice de nombres normalizados para optimizar búsqueda
        logger.info("[MATCH] Creando índice de nombres para optimizar búsqueda...")
        normalized_index = {}
        for company in table1_companies:
            normalized_name = self.normalize_company_name(company.title)
            if normalized_name not in normalized_index:
                normalized_index[normalized_name] = []
            normalized_index[normalized_name].append(company)
        
        threshold = 0.5  # Reducir umbral para mayor flexibilidad
        
        # Dividir empresas GCS en lotes para procesamiento paralelo
        batch_size = 50  # Lotes más pequeños para mejor progreso
        batches = []
        for i in range(0, len(gcs_companies), batch_size):
            batch = gcs_companies[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"[MATCH] Procesando {len(gcs_companies)} empresas en {len(batches)} lotes con 4 hilos...")
        
        matched_companies = []
        
        # Procesar lotes en paralelo
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_batch = {}
            
            # Enviar todos los lotes
            for batch_id, batch in enumerate(batches):
                future = executor.submit(self.match_gcs_batch, batch, normalized_index, threshold, batch_id + 1)
                future_to_batch[future] = batch_id + 1
            
            # Recopilar resultados
            completed_batches = 0
            total_matches = 0
            for future in as_completed(future_to_batch):
                batch_id = future_to_batch[future]
                try:
                    batch_matches = future.result()
                    matched_companies.extend(batch_matches)
                    total_matches += len(batch_matches)
                    completed_batches += 1
                    
                    progress_percent = (completed_batches / len(batches)) * 100
                    processed_companies = completed_batches * batch_size
                    logger.info(f"[MATCH] Progreso: {completed_batches}/{len(batches)} lotes ({progress_percent:.1f}%) - Empresas procesadas: {processed_companies}/{len(gcs_companies)} - Matches: {total_matches}")
                    
                except Exception as e:
                    logger.error(f"[ERROR] Error en lote {batch_id}: {str(e)}")
        
        logger.info(f"[STATS] Empresas con match exitoso: {len(matched_companies)}")
        return matched_companies

    def get_existing_companies_in_table2(self) -> set:
        """Obtener IDs de empresas que ya existen en la Tabla 2"""
        logger.info("[CHECK] Verificando empresas existentes en Tabla 2...")
        
        query = f"""
        SELECT DISTINCT id_scraping 
        FROM `{self.tabla2}`
        WHERE id_scraping IS NOT NULL
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            existing_ids = set()
            for row in results:
                existing_ids.add(row.id_scraping)
            
            logger.info(f"[STATS] Empresas ya existentes en Tabla 2: {len(existing_ids)}")
            return existing_ids
            
        except Exception as e:
            logger.error(f"[ERROR] Error obteniendo empresas existentes: {str(e)}")
            self.stats.errors_count += 1
            return set()

    def filter_new_companies(self, companies: List[CompanyData]) -> List[CompanyData]:
        """Filtrar empresas que no existen en Tabla 2"""
        if not companies:
            return []
        
        existing_ids = self.get_existing_companies_in_table2()
        
        # Filtrar empresas que no están en Tabla 2
        new_companies = [
            company for company in companies 
            if company.id_scraping not in existing_ids
        ]
        
        skipped_count = len(companies) - len(new_companies)
        
        if skipped_count > 0:
            logger.info(f"[SKIP] Se omitieron {skipped_count} empresas ya existentes en Tabla 2")
        
        logger.info(f"[FILTER] Empresas nuevas para migrar: {len(new_companies)}")
        
        return new_companies

    def migrate_to_table2(self, companies: List[CompanyData]) -> Tuple[bool, List[CompanyData]]:
        """Migrar empresas a la Tabla 2 y retornar las empresas migradas"""
        logger.info("[MIGRATE] Iniciando migración masiva a Tabla 2...")
        
        if not companies:
            logger.warning("[WARNING] No hay empresas para migrar")
            return False, []
        
        # Filtrar empresas que ya existen en Tabla 2
        new_companies = self.filter_new_companies(companies)
        
        if not new_companies:
            logger.info("[INFO] Todas las empresas ya existen en Tabla 2. No hay nada que migrar.")
            self.stats.companies_migrated_table2 = 0
            return True, []
        
        try:
            # Obtener timestamp actual en Ecuador
            current_time = datetime.now(self.ecuador_tz)
            
            # Preparar datos para inserción masiva
            rows_to_insert = []
            for company in new_companies:  # Usar new_companies filtradas
                row = {
                    'Link': company.link or None,
                    'id_scraping': company.id_scraping,
                    'Pais': company.pais or None,
                    'processed': True,
                    'is_downloaded': True,
                    'Address': company.address or None,
                    'Category': company.category or None,
                    'Email': company.email or None,
                    'Intro': company.intro or None,
                    'Phone': company.phone or None,
                    'Title': company.title or None,
                    'Created_at': current_time.strftime('%Y-%m-%d %H:%M:%S'),  # Horario Ecuador
                    'images_processed': False,
                    'company_json': False
                }
                rows_to_insert.append(row)
            
            # Obtener referencia de la tabla
            table_ref = self.bq_client.get_table(self.tabla2)
            
            # Configurar job de carga
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            
            # Ejecutar inserción masiva
            job = self.bq_client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
            job.result()  # Esperar a que termine
            
            self.stats.companies_migrated_table2 = len(new_companies)  # Usar new_companies
            logger.info(f"[OK] Migración completada exitosamente")
            logger.info(f"[STATS] Empresas migradas: {self.stats.companies_migrated_table2}")
            logger.info(f"[OK] processed=True y is_downloaded=True aplicados a todas las empresas")
            
            return True, new_companies  # Retornar empresas migradas
            
        except Exception as e:
            logger.error(f"[ERROR] Error en migración masiva: {str(e)}")
            self.stats.errors_count += 1
            return False, []

    def get_company_images(self, company_name: str, id_scraping: int) -> List[Tuple[str, str, str]]:
        """Obtener imágenes de una empresa desde GCS"""
        images = []
        bucket = self.storage_client.bucket(self.bucket_drive_name)
        company_path = f"Webscraping/Paises/Old Web Scraping/{self.folder_brasil1}/Posts/{company_name}/"
        
        try:
            # Buscar Banner.jpg
            banner_path = f"{company_path}Banner.jpg"
            if self.blob_exists(bucket, banner_path):
                images.append((banner_path, f"{id_scraping}_cover_image.jpg", "cover_image"))
            
            # Buscar Logo.jpg
            logo_path = f"{company_path}Logo.jpg"
            if self.blob_exists(bucket, logo_path):
                images.append((logo_path, f"{id_scraping}_profile_image.jpg", "profile_image"))
            
            # Buscar imágenes en Posts/
            posts_path = f"{company_path}Posts/"
            blobs = bucket.list_blobs(prefix=posts_path)
            
            post_counter = 1
            for blob in blobs:
                if blob.name.lower().endswith(('.jpg', '.jpeg', '.png')):
                    new_name = f"{id_scraping}_image{post_counter}.jpg"
                    images.append((blob.name, new_name, "post_image"))
                    post_counter += 1
            
        except Exception as e:
            logger.error(f"[ERROR] Error obteniendo imágenes de {company_name}: {str(e)}")
            self.stats.errors_count += 1
        
        return images

    def blob_exists(self, bucket, blob_name: str) -> bool:
        """Verificar si un blob existe en el bucket"""
        try:
            blob = bucket.blob(blob_name)
            return blob.exists()
        except:
            return False

    def process_company_images(self, company: CompanyData, gcs_company_name: str, index: int, total: int) -> List[ImageData]:
        """Procesar imágenes de una empresa específica"""
        try:
            # Log de progreso
            safe_title = company.title.encode('ascii', 'ignore').decode('ascii')
            logger.info(f"[PROGRESS] Procesando empresa [{index + 1}/{total}]: {safe_title}")
            
            # Obtener imágenes
            images = self.get_company_images(gcs_company_name, company.id_scraping)
            
            if not images:
                safe_title = company.title.encode('ascii', 'ignore').decode('ascii')
                logger.warning(f"[WARNING] No se encontraron imágenes para empresa {company.id_scraping} - {safe_title}")
                return []
            
            # Procesar cada imagen
            processed_images = []
            source_bucket = self.storage_client.bucket(self.bucket_drive_name)
            dest_bucket = self.storage_client.bucket(self.bucket_name)
            
            for original_path, new_name, image_type in images:
                try:
                    # Copiar imagen al bucket destino con nuevo nombre
                    source_blob = source_bucket.blob(original_path)
                    dest_blob = dest_bucket.blob(new_name)
                    
                    dest_blob.upload_from_string(
                        source_blob.download_as_bytes(),
                        content_type='image/jpeg'
                    )
                    
                    # NO usar make_public() debido a Uniform Bucket-Level Access
                    # El bucket ya debe estar configurado como público o usar IAM policies
                    
                    # Generar URL pública (funcionará si el bucket es público)
                    public_url = f"https://storage.googleapis.com/{self.bucket_name}/{new_name}"
                    
                    # Crear registro para Tabla 3
                    image_data = ImageData(
                        id_scraping=company.id_scraping,
                        country="Brasil",
                        img_path=public_url,
                        image_type=image_type,
                        id_photo_cleaned=self.get_next_photo_id()
                    )
                    processed_images.append(image_data)
                    
                except Exception as e:
                    logger.error(f"[ERROR] Error procesando imagen {original_path}: {str(e)}")
                    self.stats.errors_count += 1
            
            # Log de progreso (sanitizar nombre para evitar errores Unicode)
            safe_title = company.title.encode('ascii', 'ignore').decode('ascii')
            logger.info(f"[{index+1}/{total}] empresa: {company.id_scraping} - {safe_title} - {len(processed_images)} imágenes recuperadas, renombradas y subidas al bucket")
            
            return processed_images
            
        except Exception as e:
            logger.error(f"[ERROR] Error procesando empresa {company.title}: {str(e)}")
            self.stats.errors_count += 1
            return []

    def map_company_batch(self, companies_batch: List[CompanyData], gcs_companies: List[str], batch_id: int) -> Dict[int, str]:
        """Mapear un lote de empresas con nombres GCS"""
        batch_mapping = {}
        mapped_count = 0
        
        for company in companies_batch:
            for gcs_name in gcs_companies:
                similarity = self.calculate_similarity(gcs_name, company.title)
                if similarity >= 0.8:
                    batch_mapping[company.id_scraping] = gcs_name
                    mapped_count += 1
                    break
        
        logger.info(f"[BATCH-{batch_id}] Completado: {mapped_count}/{len(companies_batch)} empresas mapeadas")
        return batch_mapping

    def get_next_photo_id(self) -> int:
        """Obtener siguiente ID único para foto"""
        self.photo_id_counter += 1
        return self.photo_id_counter

    def process_all_images(self, matched_companies: List[CompanyData], gcs_companies: List[str]) -> List[ImageData]:
        """Procesar todas las imágenes de todas las empresas"""
        logger.info("[IMAGES] Iniciando procesamiento de imágenes...")
        logger.info(f"[INFO] Total empresas a procesar: {len(matched_companies)}")
        
        all_images = []
        total_companies = len(matched_companies)
        
        # Crear mapeo de empresas con threading
        logger.info("[MAPPING] Creando mapeo de empresas GCS...")
        logger.info(f"[MAPPING] Total empresas a mapear: {len(matched_companies)}")
        logger.info(f"[MAPPING] Total nombres GCS disponibles: {len(gcs_companies)}")
        
        company_mapping = {}
        mapped_count = 0
        total_to_map = len(matched_companies)
        
        # Dividir empresas en lotes para procesamiento paralelo
        batch_size = 100  # Lotes de 100 empresas
        batches = []
        for i in range(0, total_to_map, batch_size):
            batch = matched_companies[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"[MAPPING] Procesando en {len(batches)} lotes con {4} hilos...")
        
        # Procesar lotes en paralelo
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_batch = {}
            
            # Enviar todos los lotes
            for batch_id, batch in enumerate(batches):
                future = executor.submit(self.map_company_batch, batch, gcs_companies, batch_id + 1)
                future_to_batch[future] = batch_id + 1
            
            # Recopilar resultados
            completed_batches = 0
            for future in as_completed(future_to_batch):
                batch_id = future_to_batch[future]
                try:
                    batch_mapping = future.result()
                    company_mapping.update(batch_mapping)
                    mapped_count += len(batch_mapping)
                    completed_batches += 1
                    
                    progress_percent = (completed_batches / len(batches)) * 100
                    logger.info(f"[MAPPING] Progreso: {completed_batches}/{len(batches)} lotes ({progress_percent:.1f}%) - Total mapeadas: {mapped_count}")
                    
                except Exception as e:
                    logger.error(f"[ERROR] Error en lote {batch_id}: {str(e)}")
        
        logger.info(f"[MAPPING] Empresas mapeadas exitosamente: {mapped_count}/{len(matched_companies)}")
        logger.info(f"[INFO] Iniciando procesamiento con {5} hilos...")
        
        # Procesar imágenes con ThreadPoolExecutor para optimizar rendimiento
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_company = {}
            submitted_count = 0
            
            for i, company in enumerate(matched_companies):
                gcs_name = company_mapping.get(company.id_scraping)
                if gcs_name:
                    future = executor.submit(self.process_company_images, company, gcs_name, i, total_companies)
                    future_to_company[future] = company
                    submitted_count += 1
                    
                    # Log cada 100 tareas enviadas
                    if submitted_count % 100 == 0:
                        logger.info(f"[SUBMIT] Enviadas {submitted_count} tareas al pool de hilos...")
            
            logger.info(f"[SUBMIT] Total de {submitted_count} tareas enviadas. Esperando resultados...")
            
            # Recopilar resultados
            completed_count = 0
            for future in as_completed(future_to_company):
                completed_count += 1
                try:
                    images = future.result()
                    if images:
                        all_images.extend(images)
                        self.stats.companies_with_images += 1
                    else:
                        self.stats.companies_without_images += 1
                        
                    # Log cada 50 empresas completadas
                    if completed_count % 50 == 0:
                        logger.info(f"[COMPLETE] Completadas {completed_count}/{len(future_to_company)} empresas...")
                        
                except Exception as e:
                    company = future_to_company[future]
                    safe_title = company.title.encode('ascii', 'ignore').decode('ascii')
                    logger.error(f"[ERROR] Error en procesamiento de empresa {company.id_scraping} - {safe_title}: {str(e)}")
                    self.stats.errors_count += 1
                    self.stats.companies_without_images += 1
        
        self.stats.total_images_processed = len(all_images)
        logger.info(f"[OK] Procesamiento de imágenes completado")
        logger.info(f"[STATS] Total imágenes procesadas: {self.stats.total_images_processed}")
        logger.info(f"[STATS] Empresas CON imágenes: {self.stats.companies_with_images}")
        logger.info(f"[STATS] Empresas SIN imágenes: {self.stats.companies_without_images}")
        
        return all_images

    def get_existing_images_in_table3(self) -> set:
        """Obtener paths de imágenes que ya existen en la Tabla 3"""
        logger.info("[CHECK] Verificando imágenes existentes en Tabla 3...")
        
        query = f"""
        SELECT DISTINCT img_path 
        FROM `{self.tabla3}`
        WHERE img_path IS NOT NULL
        """
        
        try:
            query_job = self.bq_client.query(query)
            results = query_job.result()
            
            existing_paths = set()
            for row in results:
                existing_paths.add(row.img_path)
            
            logger.info(f"[STATS] Imágenes ya existentes en Tabla 3: {len(existing_paths)}")
            return existing_paths
            
        except Exception as e:
            logger.error(f"[ERROR] Error obteniendo imágenes existentes: {str(e)}")
            self.stats.errors_count += 1
            return set()

    def filter_new_images(self, images: List[ImageData]) -> List[ImageData]:
        """Filtrar imágenes que no existen en Tabla 3"""
        if not images:
            return []
        
        existing_paths = self.get_existing_images_in_table3()
        
        # Filtrar imágenes que no están en Tabla 3
        new_images = [
            image for image in images 
            if image.img_path not in existing_paths
        ]
        
        skipped_count = len(images) - len(new_images)
        
        if skipped_count > 0:
            logger.info(f"[SKIP] Se omitieron {skipped_count} imágenes ya existentes en Tabla 3")
        
        logger.info(f"[FILTER] Imágenes nuevas para insertar: {len(new_images)}")
        
        return new_images

    def insert_images_to_table3(self, images: List[ImageData]) -> bool:
        """Insertar imágenes en la Tabla 3"""
        logger.info("[INSERT] Insertando imágenes en Tabla 3...")
        
        if not images:
            logger.warning("[WARNING] No hay imágenes para insertar")
            return False
        
        # Filtrar imágenes que ya existen en Tabla 3
        new_images = self.filter_new_images(images)
        
        if not new_images:
            logger.info("[INFO] Todas las imágenes ya existen en Tabla 3. No hay nada que insertar.")
            return True
        
        try:
            # Obtener timestamp actual en Ecuador
            current_time = datetime.now(self.ecuador_tz)
            
            # Preparar datos para inserción masiva
            rows_to_insert = []
            for image in new_images:  # Usar new_images filtradas
                row = {
                    'id_scraping': image.id_scraping,
                    'country': image.country,
                    'img_path': image.img_path,
                    'image_type': image.image_type,
                    'created_at': current_time.strftime('%Y-%m-%d %H:%M:%S'),  # Horario Ecuador
                    'id_photo_cleaned': image.id_photo_cleaned,
                    'product_information': None,
                    'token_input': None,
                    'token_output': None,
                    'model_used': None,
                    'execution_time_seconds': None,
                    'processed_ia_at': None,
                    'time_out': None,
                    'segment': None,
                    'type_process': None,
                    'batch_selected': False,
                    'token_think': None
                }
                rows_to_insert.append(row)
            
            # Obtener referencia de la tabla
            table_ref = self.bq_client.get_table(self.tabla3)
            
            # Configurar job de carga
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            
            # Ejecutar inserción masiva
            job = self.bq_client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
            job.result()  # Esperar a que termine
            
            logger.info(f"[OK] Inserción en Tabla 3 completada exitosamente")
            logger.info(f"[STATS] Imágenes insertadas: {len(new_images)}")  # Usar new_images
            
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Error en inserción masiva a Tabla 3: {str(e)}")
            self.stats.errors_count += 1
            return False

    def print_final_stats(self, start_time: float):
        """Imprimir estadísticas finales del proceso"""
        end_time = time.time()
        self.stats.execution_time_seconds = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("[STATS] ESTADÍSTICAS FINALES DEL PROCESO")
        logger.info("=" * 60)
        logger.info(f"[STATS] Empresas encontradas en GCS: {self.stats.companies_found_gcs}")
        logger.info(f"[STATS] Empresas encontradas en Tabla 1: {self.stats.companies_found_table1}")
        logger.info(f"[STATS] Empresas migradas a Tabla 2: {self.stats.companies_migrated_table2}")
        logger.info(f"[STATS] Empresas CON imagenes procesadas: {self.stats.companies_with_images}")
        logger.info(f"[STATS] Empresas SIN imagenes: {self.stats.companies_without_images}")
        logger.info(f"[STATS] Total imágenes procesadas: {self.stats.total_images_processed}")
        logger.info(f"[STATS] Errores durante el proceso: {self.stats.errors_count}")
        logger.info(f"[STATS] Tiempo total de ejecución: {self.stats.execution_time_seconds:.2f} segundos")
        logger.info(f"[STATS] Tiempo total de ejecución: {self.stats.execution_time_seconds/60:.2f} minutos")
        logger.info("=" * 60)
        
        # Tabla de estadísticas
        print("\n" + "=" * 80)
        print("                    RESUMEN ESTADÍSTICAS FINALES")
        print("=" * 80)
        print(f"{'Métrica':<35} {'Valor':<20} {'Detalles':<25}")
        print("-" * 80)
        print(f"{'Empresas en GCS':<35} {self.stats.companies_found_gcs:<20} {'Encontradas':<25}")
        print(f"{'Empresas en Tabla 1':<35} {self.stats.companies_found_table1:<20} {'Brasil':<25}")
        print(f"{'Empresas migradas':<35} {self.stats.companies_migrated_table2:<20} {'A Tabla 2':<25}")
        print(f"{'Empresas CON imágenes':<35} {self.stats.companies_with_images:<20} {'Procesadas':<25}")
        print(f"{'Empresas SIN imágenes':<35} {self.stats.companies_without_images:<20} {'Sin procesar':<25}")
        print(f"{'Imágenes procesadas':<35} {self.stats.total_images_processed:<20} {'URLs públicas':<25}")
        print(f"{'Errores':<35} {self.stats.errors_count:<20} {'Durante proceso':<25}")
        print(f"{'Tiempo ejecución':<35} {f'{self.stats.execution_time_seconds:.2f}s':<20} {f'{self.stats.execution_time_seconds/60:.2f} min':<25}")
        print("=" * 80)

    def run_migration_process(self):
        """Ejecutar el proceso completo de migración"""
        start_time = time.time()
        logger.info("[START] INICIANDO PROCESO DE MIGRACIÓN COMPLETO")
        logger.info("=" * 60)
        
        try:
            # Fase 1: Obtener empresas de GCS
            logger.info("[FASE 1] Obtención de empresas desde GCS")
            gcs_companies = self.get_companies_from_gcs()
            
            if not gcs_companies:
                logger.error("[ERROR] No se encontraron empresas en GCS. Proceso abortado.")
                return
            
            # Fase 2: Obtener empresas de Tabla 1
            logger.info("[FASE 2] Obtención de empresas desde Tabla 1")
            table1_companies = self.get_companies_from_table1()
            
            if not table1_companies:
                logger.error("[ERROR] No se encontraron empresas en Tabla 1. Proceso abortado.")
                return
            
            # Fase 3: Comparar y hacer match
            logger.info("[FASE 3] Comparación y matching de empresas")
            matched_companies = self.match_companies(gcs_companies, table1_companies)
            
            if not matched_companies:
                logger.error("[ERROR] No se encontraron matches entre GCS y Tabla 1. Proceso abortado.")
                return
            
            # Fase 4: Migrar a Tabla 2
            logger.info("[FASE 4] Migración masiva a Tabla 2")
            migration_success, migrated_companies = self.migrate_to_table2(matched_companies)
            
            if not migration_success:
                logger.error("[ERROR] Error en migración a Tabla 2. Proceso abortado.")
                return
            
            # Fase 5: Procesar imágenes SOLO de empresas migradas
            if migrated_companies:
                logger.info("[FASE 5] Procesamiento de imágenes")
                processed_images = self.process_all_images(migrated_companies, gcs_companies)
                
                if not processed_images:
                    logger.warning("[WARNING] No se procesaron imágenes, pero el proceso continúa.")
                
                # Fase 6: Insertar en Tabla 3
                if processed_images:
                    logger.info("[FASE 6] Inserción de imágenes en Tabla 3")
                    insert_success = self.insert_images_to_table3(processed_images)
                    
                    if not insert_success:
                        logger.error("[ERROR] Error en inserción a Tabla 3.")
            else:
                logger.info("[INFO] No hay empresas nuevas para procesar imágenes.")
            
            # Mostrar estadísticas finales
            self.print_final_stats(start_time)
            
            logger.info("[SUCCESS] PROCESO DE MIGRACIÓN COMPLETADO EXITOSAMENTE")
            
        except Exception as e:
            logger.error(f"[ERROR] Error crítico en el proceso de migración: {str(e)}")
            self.stats.errors_count += 1
            self.print_final_stats(start_time)

def main():
    """Función principal"""
    try:
        processor = DataMigrationProcessor()
        processor.run_migration_process()
    except Exception as e:
        logger.error(f"[ERROR] Error en inicialización: {str(e)}")
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
