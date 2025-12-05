# -*- coding: utf-8 -*-
"""
AI Photo Vertex - Procesamiento de imágenes con Vertex AI para marketplace de construcción
Analiza imágenes desde Cloud Storage usando Vertex AI Gemini para clasificar productos de construcción
"""

import os
import json
import logging
import time
import threading
from typing import Dict, List, Optional, Tuple
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig
from dotenv import load_dotenv

# Configurar logging con formato más limpio (solo consola)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Silenciar logs de debug de Google Cloud
logging.getLogger('google').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Cargar variables de entorno
load_dotenv()

class AIPhotoVertexProcessor:
    def get_next_pending_id_scraping(self) -> Optional[int]:
        """
        Obtiene el siguiente id_scraping que tiene al menos una imagen pendiente de procesar
        """
        try:
            query = f"""
            SELECT id_scraping, COUNT(*) as pending_images
            FROM `{self.cleaned_table}`
            WHERE image_type = 'post_image' 
            AND is_construction_image IS NULL
            AND (time_out IS NULL OR time_out = FALSE)
            GROUP BY id_scraping
            HAVING COUNT(*) > 0
            ORDER BY id_scraping ASC
            LIMIT 1
            """
            query_job = self.bq_client.query(query)
            results = query_job.result()
            for row in results:
                logger.info(f"Empresa encontrada: {row.id_scraping} ({row.pending_images} imágenes pendientes)")
                return row.id_scraping
            logger.info("No hay empresas pendientes")
            return None
        except Exception as e:
            logger.error(f"Error al buscar empresa: {str(e)}")
            return None
    
    def verify_company_completion(self, id_scraping: int) -> bool:
        """
        Verifica si una empresa ha sido completamente procesada
        
        Args:
            id_scraping: ID de la empresa a verificar
            
        Returns:
            True si todas las imágenes han sido procesadas, False si aún hay pendientes
        """
        try:
            query = f"""
            SELECT COUNT(*) as pending_count
            FROM `{self.cleaned_table}`
            WHERE image_type = 'post_image' 
              AND is_construction_image IS NULL
              AND id_scraping = @id_scraping
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("id_scraping", "INT64", id_scraping),
                ]
            )
            
            query_job = self.bq_client.query(query, job_config=job_config)
            results = query_job.result()
            
            for row in results:
                pending_count = row.pending_count
                logger.debug(f"Empresa id_scraping={id_scraping} tiene {pending_count} imágenes pendientes")
                return pending_count == 0
                
            return True  # Si no hay resultados, asumimos que está completa
            
        except Exception as e:
            logger.error(f"Error al verificar completitud de empresa {id_scraping}: {str(e)}")
            return False
    
    def get_company_context(self, id_scraping: int) -> Dict[str, str]:
        """
        Obtiene el contexto de la empresa (title e intro) desde la tabla raw
        
        Args:
            id_scraping: ID de la empresa
            
        Returns:
            Diccionario con title e intro de la empresa
        """
        try:
            query = """
            SELECT title, intro 
            FROM `web-scraping-468121.web_scraping_raw_data.mx_web_scraping_raw_update_copy_new` 
            WHERE id_scraping = @id_scraping
            LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("id_scraping", "INT64", id_scraping),
                ]
            )
            
            query_job = self.bq_client.query(query, job_config=job_config)
            results = query_job.result()
            
            for row in results:
                return {
                    'title': row.title or 'Empresa sin nombre',
                    'intro': row.intro or 'Sin descripción disponible'
                }
                
            # Si no se encuentra la empresa, devolver valores por defecto
            return {
                'title': 'Empresa sin nombre',
                'intro': 'Sin descripción disponible'
            }
            
        except Exception as e:
            logger.error(f"Error al obtener contexto de empresa {id_scraping}: {str(e)}")
            return {
                'title': 'Empresa sin nombre',
                'intro': 'Sin descripción disponible'
            }
    
    def update_company_images_processed(self, id_scraping: int, images_processed: bool = True):
        """
        Actualizar el flag images_processed en la tabla raw para indicar que la empresa fue procesada
        
        Args:
            id_scraping: ID de la empresa
            images_processed: True si se procesaron imágenes, False si no
        """
        try:
            query = """
            UPDATE `web-scraping-468121.web_scraping_raw_data.mx_web_scraping_raw_update_copy_new`
            SET images_processed = @images_processed
            WHERE id_scraping = @id_scraping
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("images_processed", "BOOL", images_processed),
                    bigquery.ScalarQueryParameter("id_scraping", "INT64", id_scraping),
                ]
            )
            
            query_job = self.bq_client.query(query, job_config=job_config)
            query_job.result()  # Esperar a que termine
            
            logger.info(f"Flag images_processed={images_processed} actualizado para empresa {id_scraping}")
            
        except Exception as e:
            logger.error(f"Error actualizando flag images_processed para empresa {id_scraping}: {str(e)}")
            # No lanzamos excepción para que no afecte el flujo principal
    
    def __init__(self):
        """Inicializar el procesador de imágenes con Vertex AI"""
        
        # Configuración desde .env
        self.service_account_file = os.getenv('SERVICE_ACCOUNT_FILE')  # Para BigQuery y Storage
        self.service_account_file_vertex = os.getenv('SERVICE_ACCOUNT_FILE_VERTEX')  # Para Vertex AI
        self.project_id = os.getenv('PROJECT_ID')  # web-scraping-468121
        self.cleaned_table = os.getenv('CLEANED_TABLE')
        
        # Configuración Vertex AI - PONER EN ENV
        self.vertex_project_id = os.getenv('VERTEX_PROJECT_ID')  # Proyecto de Vertex AI
        self.location = os.getenv('LOCATION')
        self.model_id = os.getenv('MODEL_ID')
        
        # Inicializar credenciales y clientes
        self._setup_credentials()
        self._setup_clients()
        self._setup_vertex_model()
        self._setup_prompt()
        
    def _setup_credentials(self):
        """Configurar credenciales de Google Cloud"""
        try:
            # Credenciales para BigQuery y Cloud Storage
            if not os.path.exists(self.service_account_file):
                raise FileNotFoundError(f"Archivo de credenciales principal no encontrado: {self.service_account_file}")
            
            self.credentials = service_account.Credentials.from_service_account_file(
                self.service_account_file
            )
            
            # Credenciales para Vertex AI
            if not os.path.exists(self.service_account_file_vertex):
                raise FileNotFoundError(f"Archivo de credenciales Vertex no encontrado: {self.service_account_file_vertex}")
            
            self.credentials_vertex = service_account.Credentials.from_service_account_file(
                self.service_account_file_vertex
            )
            
        except Exception as e:
            logger.error(f"Error al cargar credenciales: {str(e)}")
            raise
    
    def _setup_clients(self):
        """Inicializar clientes de Google Cloud"""
        try:
            # Cliente BigQuery - usa credenciales principales
            self.bq_client = bigquery.Client(
                project=self.project_id,
                credentials=self.credentials
            )
            
            # Cliente Cloud Storage - usa credenciales principales  
            self.storage_client = storage.Client(
                project=self.project_id,
                credentials=self.credentials
            )
            
        except Exception as e:
            logger.error(f"Error al inicializar clientes: {str(e)}")
            raise
    
    def _setup_vertex_model(self):
        """Configurar modelo Vertex AI"""
        try:
            # Inicializar Vertex AI con credenciales específicas
            vertexai.init(
                project=self.vertex_project_id,
                location=self.location,
                credentials=self.credentials_vertex
            )
            
            # Configurar modelo
            self.model = GenerativeModel(self.model_id)
            
            # Configuración de generación JSON
            self.json_schema = {
                "type": "OBJECT",
                "properties": {
                    "products": {
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "product_name": {"type": "STRING"},
                                "sku": {"type": "STRING", "nullable": True},
                                "model": {"type": "STRING", "nullable": True},
                                "price": {"type": "NUMBER", "nullable": True},
                                "currency": {"type": "STRING", "nullable": True},
                                "brand": {"type": "STRING", "nullable": True},
                                "category": {"type": "STRING"},
                                "product_description": {"type": "STRING"},
                                "specifications": {"type": "STRING", "nullable": True},
                                "product_image": {"type": "STRING", "nullable": True}
                            },
                            "required": ["product_name", "category", "product_description"]
                        }
                    }
                },
                "required": ["products"]
            }
            
            self.generation_config = GenerationConfig(
                temperature=0.0,
                response_mime_type="application/json",
                response_schema=self.json_schema,
            )
            
        except Exception as e:
            logger.error(f"Error al configurar Vertex AI: {str(e)}")
            raise
    
    def _setup_prompt(self):
        """Definir el prompt para análisis de imágenes"""
        self.prompt_json = (
            "Eres un asistente de IA experto en la catalogación de productos para un marketplace enfocado EXCLUSIVAMENTE en construcción, arquitectura, diseño e industrial (manufactura)."
            "\n\n"
            "**CONTEXTO CRÍTICO: Las imágenes que analizarás provienen SIEMPRE de publicaciones en redes sociales.** Esto significa que a menudo serán publicidades, promociones, anuncios o escaparates de marca, y rara vez fichas técnicas."
            "\n\n"
            "REGLA CRÍTICA: Tu tarea principal es analizar estas imágenes de redes sociales y extraer ÚNICAMENTE productos o líneas de producto que pertenezcan a categorías de construcción, arquitectura, diseño o industriales. **Debes ser capaz de identificar productos relevantes incluso si se presentan en un formato de anuncio o banner.**"
            "\n\n"
            "REGLAS DE INCLUSIÓN Y EXCLUSIÓN:"
            "1.  **INCLUIR:** Busca activamente materiales (ej: aceros, perfiles de aluminio, cementos), herramientas (ej: taladros), componentes (ej: tornillería), maquinaria, y acabados (ej: pisos, luminarias), diseño interior, etc."
            "2.  **EXCLUIR:** IGNORA categóricamente cualquier producto de otras industrias (ej: moda, alimentos, electrónica de consumo, papelería general)."
            "3.  **MANEJO DE PUBLICACIONES (REDES SOCIALES):** Si la imagen es una publicidad o una promoción (ej: '20% Descuento en Pisos'), o la imagen de un post, **SÍ DEBES EXTRAERLA** siempre que sea de una industria relevante a la construcción o diseño de interiores o materiales industriales "
                "   - **Trata la publicación como un solo producto.**"
                "   - El product_name debe ser descriptivo (ej: 'Promoción de Pisos Laminados' o 'Soluciones de Perfiles de Aluminio Ancomex')."
                "   - **La información (descripción, especificaciones) debe extraerse del TEXTO DENTRO de la propia imagen.**"
            "\n\n"
            "FORMATO DE SALIDA: Responde SIEMPRE y ÚNICAMENTE con un objeto JSON válido (application/json) que contenga una clave 'products', la cual será una lista de objetos. Si no encuentras productos relevantes, la lista debe estar vacía."
            "\n\n"
            "REGLAS PARA CADA PRODUCTO EN LA LISTA:"
            " 'product_name': {"
            "  'description': 'Obligatorio. String. Máximo 250 caracteres. Debe ser un título en español, altamente descriptivo y optimizado para SEO, basado en el texto de la imagen. Usa la fórmula deseada: [Nombre Genérico del Producto] + [Material o Característica Principal] + [Marca (si existe)] + [Dimensiones o Modelo]. Ejemplo: \\'Placa de Acero al Carbón 1500x3000mm\\'. **Si es un anuncio de marca, usa: \\'Soluciones de [Producto] [Marca]\\'.**'"
            " },"
            " 'sku': {"
            "  'description': 'Opcional. String. SKU del producto. **Dado que son redes sociales, este valor casi siempre será null.**'"
            " },"
            " 'model': {"
            "  'description': 'Opcional. String. El modelo del producto, si aplica. **Dado que son redes sociales, este valor a menudo será null.**'"
            " },"
            " 'price': {"
            "  'description': 'Obligatorio. Número (float o integer). **Busca activamente un precio en el texto de la imagen.** Si no se encuentra explícitamente, el valor debe ser null.'"
            " },"
            " 'currency': {"
            "  'description': 'Obligatorio. String. El código de moneda de 3 letras (ej: USD, EUR, MXN). Si no se especifica junto al precio, el valor debe ser null.'"
            " },"
            " 'brand': {"
            "  'description': 'Opcional. String. La marca del producto. **Intenta identificar la marca desde el logo o texto en la imagen.** Si no se identifica claramente, el valor debe ser null.'"
            " },"
            " 'category': {"
            "  'description': 'Obligatorio. String. La categoría del producto en el sector construcción. Ejemplo: \\'Aceros y Metales\\'.'"
            " },"
            " 'product_description': {"
            "  'description': 'Obligatorio. String. Párrafo en español orientado a SEO máximo 1000 caracteres. **Utiliza el texto promocional o descriptivo encontrado en la imagen para construir este párrafo.** Debe describir los beneficios y usos para profesionales del sector (arquitectos, ingenieros, contratistas).'"
            " },"
            " 'specifications': {"
            "  'description': 'Opcional. String. Máximo 250 caracteres. **Extrae las especificaciones técnicas clave (medidas, materiales) DEL TEXTO dentro de la imagen.** Formato: \\'Especificación 1: Valor 1; Especificación 2: Valor 2\\'.'"
            " },"
            " 'product_image': {"
            "  'description': 'Obligatorio. String. La URL o ruta real de la imagen del producto (la imagen que estás analizando). Si no existe, debe ser null.'"
            " }"
            "}"
            "\n\n"
            "CASO SIN RESULTADOS: Si después de aplicar el filtro no hay ningún producto o marca relevante (según la REGLA DE EXCLUSIÓN), responde estrictamente con: {\"products\": []}"
        )
    
    def get_images_to_process(self, limit: int = 1, id_scraping: Optional[int] = None) -> List[Dict]:
        """
        Obtener imágenes desde BigQuery para procesar
        
        Args:
            limit: Número de imágenes a procesar (no usado cuando se especifica id_scraping)
            id_scraping: ID de la empresa específica a procesar
            
        Returns:
            Lista de diccionarios con img_path e id_photo_cleaned
        """
        try:
            if id_scraping is not None:
                query = f"""
                SELECT img_path, id_photo_cleaned 
                FROM `{self.cleaned_table}`
                WHERE image_type = 'post_image' 
                  AND is_construction_image IS NULL
                  AND (time_out IS NULL OR time_out = FALSE)
                  AND id_scraping = @id_scraping
                ORDER BY id_photo_cleaned ASC
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("id_scraping", "INT64", id_scraping),
                    ]
                )
                query_job = self.bq_client.query(query, job_config=job_config)
            else:
                query = f"""
                SELECT img_path, id_photo_cleaned 
                FROM `{self.cleaned_table}`
                WHERE image_type = 'post_image' 
                  AND is_construction_image IS NULL
                  AND (time_out IS NULL OR time_out = FALSE)
                ORDER BY id_photo_cleaned ASC
                LIMIT {limit}
                """
                query_job = self.bq_client.query(query)
                
            results = query_job.result()
            images = []
            for row in results:
                images.append({
                    'img_path': row.img_path,
                    'id_photo_cleaned': row.id_photo_cleaned
                })
                
            return images
            
        except Exception as e:
            logger.error(f"Error al obtener imágenes: {str(e)}")
            raise
    
    def analyze_image_with_timeout(self, img_url: str, company_context: Dict[str, str] = None, timeout_seconds: int = 60) -> Tuple[bool, Optional[Dict], Dict]:
        """
        Analizar imagen con timeout para evitar bloqueos prolongados
        
        Args:
            img_url: URL de la imagen en Cloud Storage
            company_context: Contexto de la empresa (title e intro)
            timeout_seconds: Tiempo máximo en segundos para la categorización
            
        Returns:
            Tupla (es_construccion, datos_producto, metadata_ai) o (None, None, None) si hay timeout
        """
        result = [None]  # Lista para almacenar el resultado del hilo
        exception_occurred = [None]  # Lista para almacenar excepciones
        
        def worker():
            try:
                result[0] = self.analyze_image_with_vertex(img_url, company_context)
            except Exception as e:
                exception_occurred[0] = e
        
        # Crear y iniciar el hilo
        thread = threading.Thread(target=worker)
        thread.daemon = True
        thread.start()
        
        # Esperar por el resultado con timeout
        thread.join(timeout=timeout_seconds)
        
        if thread.is_alive():
            # El hilo aún está corriendo - timeout
            logger.warning(f"TIMEOUT: Imagen {img_url} excedió {timeout_seconds}s - SALTANDO")
            return None, None, None
        
        if exception_occurred[0]:
            # Hubo una excepción en el hilo
            raise exception_occurred[0]
        
        return result[0]
    
    def analyze_image_with_vertex(self, img_url: str, company_context: Dict[str, str] = None) -> Tuple[bool, Optional[Dict], Dict]:
        """
        Analizar imagen con Vertex AI Gemini directamente desde Cloud Storage
        
        Args:
            img_url: URL de la imagen en Cloud Storage
            company_context: Contexto de la empresa (title e intro)
            
        Returns:
            Tupla (es_construccion, datos_producto, metadata_ai)
        """
        metadata_ai = {
            'token_input': 0,
            'token_output': 0,
            'model_used': self.model_id
        }
        
        try:
            # Crear Part de imagen desde URI de Cloud Storage
            image_part = Part.from_uri(img_url, mime_type="image/jpeg")
            
            # Crear prompt contextualizado
            contextualized_prompt = self.prompt_json
            if company_context:
                context_addition = (
                    f"\n\n**CONTEXTO DE LA EMPRESA:**"
                    f"\n- Nombre de la empresa: {company_context.get('title', 'No disponible')}"
                    f"\n- Descripción de la empresa: {company_context.get('intro', 'No disponible')}"
                    f"\n\nUsa este contexto para mejor identificación de productos y marcas en la imagen."
                )
                contextualized_prompt = self.prompt_json + context_addition
            
            # Generar contenido
            response = self.model.generate_content(
                [contextualized_prompt, image_part],
                generation_config=self.generation_config
            )
            
            # Extraer información de tokens si está disponible
            try:
                if hasattr(response, 'usage_metadata') and response.usage_metadata:
                    metadata_ai['token_input'] = response.usage_metadata.prompt_token_count if hasattr(response.usage_metadata, 'prompt_token_count') else 0
                    metadata_ai['token_output'] = response.usage_metadata.candidates_token_count if hasattr(response.usage_metadata, 'candidates_token_count') else 0
                elif hasattr(response, '_raw_response'):
                    # Intento alternativo para extraer tokens
                    raw_response = response._raw_response
                    if hasattr(raw_response, 'usage_metadata'):
                        usage = raw_response.usage_metadata
                        metadata_ai['token_input'] = getattr(usage, 'prompt_token_count', 0)
                        metadata_ai['token_output'] = getattr(usage, 'candidates_token_count', 0)
            except Exception as token_error:
                logger.debug(f"No se pudieron extraer tokens: {str(token_error)}")
            
            # Parsear respuesta JSON
            result_text = response.text
            
            try:
                result_json = json.loads(result_text)
            except json.JSONDecodeError as e:
                logger.error(f"Error JSON: {str(e)}")
                return False, None, metadata_ai
            
            # Verificar si hay productos válidos
            products = result_json.get('products', [])
            
            if not products or len(products) == 0:
                return False, None, metadata_ai
            
            # Si hay productos, tomar el primero y añadir la URL de la imagen
            product = products[0]
            product['product_image'] = img_url
            
            return True, product, metadata_ai
            
        except Exception as e:
            logger.error(f"Error Vertex AI: {str(e)}")
            return False, None, metadata_ai
    
    def update_image_classification(self, id_photo_cleaned: int, is_construction: bool, product_info: Optional[Dict] = None, metadata_ai: Dict = None, execution_time_seconds: int = 0):
        """
        Actualizar la clasificación de la imagen en BigQuery
        
        Args:
            id_photo_cleaned: ID único de la imagen
            is_construction: Si es imagen de construcción
            product_info: Información del producto (si aplica)
            metadata_ai: Metadata de IA con tokens y modelo usado
            execution_time_seconds: Tiempo de ejecución en segundos
        """
        try:
            # Preparar el JSON del producto
            product_json = None
            if product_info:
                product_json = json.dumps(product_info, ensure_ascii=False)
            
            # Preparar metadata de AI con valores por defecto
            if metadata_ai is None:
                metadata_ai = {'token_input': 0, 'token_output': 0, 'model_used': self.model_id}
            
            # Query de actualización incluyendo processed_ia_at
            query = f"""
            UPDATE `{self.cleaned_table}`
            SET 
                is_construction_image = @is_construction,
                product_information = @product_info,
                token_input = @token_input,
                token_output = @token_output,
                model_used = @model_used,
                execution_time_seconds = @execution_time_seconds,
                processed_ia_at = TIMESTAMP(DATETIME_TRUNC(CURRENT_DATETIME('America/Guayaquil'), SECOND))
            WHERE id_photo_cleaned = @id_photo_cleaned
            """
            # Configurar parámetros
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("is_construction", "BOOL", is_construction),
                    bigquery.ScalarQueryParameter("product_info", "STRING", product_json),
                    bigquery.ScalarQueryParameter("token_input", "INT64", metadata_ai.get('token_input', 0)),
                    bigquery.ScalarQueryParameter("token_output", "INT64", metadata_ai.get('token_output', 0)),
                    bigquery.ScalarQueryParameter("model_used", "STRING", metadata_ai.get('model_used', self.model_id)),
                    bigquery.ScalarQueryParameter("execution_time_seconds", "INT64", execution_time_seconds),
                    bigquery.ScalarQueryParameter("id_photo_cleaned", "INT64", id_photo_cleaned),
                ]
            )
            # Ejecutar query
            query_job = self.bq_client.query(query, job_config=job_config)
            query_job.result()  # Esperar a que termine
            
        except Exception as e:
            logger.error(f"Error actualizando BD: {str(e)}")
            raise
    
    def process_single_image(self, img_data: Dict, company_context: Dict[str, str] = None) -> bool:
        """
        Procesar una sola imagen con manejo robusto de errores y timeout
        
        Args:
            img_data: Diccionario con img_path e id_photo_cleaned
            company_context: Contexto de la empresa (title e intro)
            
        Returns:
            True si se procesó correctamente, False en caso contrario, None si hay timeout
        """
        img_path = img_data['img_path']
        id_photo_cleaned = img_data['id_photo_cleaned']
        # Mostrar siempre id_photo_cleaned y img_path en el log
        logger.info(f"Procesando imagen: id_photo_cleaned={id_photo_cleaned}, img_path={img_path}")

        # Iniciar medición de tiempo
        start_time = time.time()

        try:
            # Analizar con timeout de 60 segundos
            result = self.analyze_image_with_timeout(img_path, company_context, timeout_seconds=60)

            # Calcular tiempo de ejecución
            execution_time_seconds = int(time.time() - start_time)

            # Verificar si hubo timeout
            if result[0] is None and result[1] is None and result[2] is None:
                logger.warning(f"[TIMEOUT] Imagen {id_photo_cleaned} - NO se actualiza BD")
                logger.info(f"[TIMEOUT] img_path={img_path}")
                # Marcar time_out=True en BD
                try:
                    query = f"""
                    UPDATE `{self.cleaned_table}`
                    SET time_out = TRUE
                    WHERE id_photo_cleaned = @id_photo_cleaned
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("id_photo_cleaned", "INT64", id_photo_cleaned),
                        ]
                    )
                    self.bq_client.query(query, job_config=job_config).result()
                    logger.info(f"Marcado time_out=True para imagen {id_photo_cleaned}")
                except Exception as e:
                    logger.error(f"Error marcando time_out en BD: {str(e)}")
                return None  # Indicar que hubo timeout

            is_construction, product_info, metadata_ai = result

            # Actualizar base de datos con tiempo de ejecución
            self.update_image_classification(id_photo_cleaned, is_construction, product_info, metadata_ai, execution_time_seconds)

            if is_construction:
                logger.info(f"[CONSTRUCCION] {product_info.get('product_name', 'Sin nombre')} | {product_info.get('category', 'Sin categoría')}")
                logger.info(f"Tokens: IN={metadata_ai.get('token_input', 0)} OUT={metadata_ai.get('token_output', 0)} | Modelo: {metadata_ai.get('model_used', 'N/A')} | Tiempo: {execution_time_seconds}s")
            else:
                logger.info(f"[NO-CONSTRUCCION] Imagen no es de construcción")
                logger.info(f"Tokens: IN={metadata_ai.get('token_input', 0)} OUT={metadata_ai.get('token_output', 0)} | Modelo: {metadata_ai.get('model_used', 'N/A')} | Tiempo: {execution_time_seconds}s")

            return is_construction

        except Exception as e:
            # Calcular tiempo de ejecución incluso en caso de error
            execution_time_seconds = int(time.time() - start_time)
            logger.error(f"Error procesando imagen {id_photo_cleaned}: {str(e)}")
            logger.info(f"[ERROR] img_path={img_path}")
            # Marcar como no procesable con tiempo de ejecución
            try:
                metadata_ai_error = {'token_input': 0, 'token_output': 0, 'model_used': self.model_id}
                self.update_image_classification(id_photo_cleaned, False, None, metadata_ai_error, execution_time_seconds)
            except Exception as db_error:
                logger.error(f"Error actualizando BD: {str(db_error)}")

            return False
    
    def process_images_batch(self, batch_size: int = 10):
        """
        Procesar un lote de imágenes de una empresa
        
        Args:
            batch_size: Número de imágenes a procesar en el lote (no usado, procesa toda la empresa)
        """
        start_time = time.time()
        
        try:
            # Buscar una empresa pendiente con imágenes por procesar
            id_scraping = self.get_next_pending_id_scraping()
            if id_scraping is None:
                logger.info("No hay empresas pendientes")
                return
                
            # Verificar si la empresa ya fue procesada (doble verificación)
            if self.verify_company_completion(id_scraping):
                logger.warning(f"Empresa {id_scraping} ya procesada")
                return
                
            # Obtener todas las imágenes pendientes de la empresa
            images = self.get_images_to_process(id_scraping=id_scraping)
            if not images:
                logger.info(f"No hay imágenes pendientes para empresa {id_scraping}")
                return
            
            logger.info(f"Procesando empresa {id_scraping} - {len(images)} imágenes")
            
            # Obtener contexto de la empresa
            company_context = self.get_company_context(id_scraping)
            logger.info(f"Empresa: {company_context['title']}")
            
            # Contadores para estadísticas
            processed_count = 0
            success_count = 0
            error_count = 0
            timeout_count = 0
            
            # Procesar cada imagen de la empresa
            for i, img_data in enumerate(images, 1):
                img_start_time = time.time()
                logger.info(f"--- Imagen {i}/{len(images)} ---")
                logger.info(f"ID: {img_data['id_photo_cleaned']} | URL: {img_data['img_path']}")
                
                try:
                    result = self.process_single_image(img_data, company_context)
                    
                    if result is None:
                        # Timeout - no se procesó ni actualizó BD
                        timeout_count += 1
                    elif result:
                        # Procesada exitosamente
                        processed_count += 1
                        success_count += 1
                    else:
                        # Error durante procesamiento
                        processed_count += 1
                        error_count += 1
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error crítico en imagen {img_data['id_photo_cleaned']}: {str(e)}")
                    continue
                
                # Pausa entre imágenes
                if i < len(images):
                    time.sleep(1)
            
            # Verificación final
            if self.verify_company_completion(id_scraping):
                logger.info(f"[COMPLETADO] Empresa {id_scraping} terminada")
            else:
                logger.warning(f"[ADVERTENCIA] Empresa {id_scraping} tiene imágenes pendientes")
            
            # Resumen
            total_time = time.time() - start_time
            logger.info(f"=== RESUMEN ===")
            logger.info(f"Empresa: {id_scraping}")
            logger.info(f"Tiempo total: {total_time:.1f}s ({total_time/60:.1f}min)")
            logger.info(f"Procesadas: {processed_count}/{len(images)}")
            logger.info(f"Exitosas: {success_count} | Errores: {error_count} | Timeouts: {timeout_count}")
            
            if total_time > 0:
                rate = (len(images) * 60) / total_time
                logger.info(f"Velocidad: {rate:.1f} img/min")
            
            # Actualizar flag images_processed en la tabla raw
            # Se marca como procesada si se procesó al menos una imagen exitosamente
            if processed_count > 0 or success_count > 0:
                self.update_company_images_processed(id_scraping, True)
                logger.info(f"[FLAG ACTUALIZADO] Empresa {id_scraping} marcada como procesada")
            else:
                logger.warning(f"[FLAG NO ACTUALIZADO] Empresa {id_scraping} no tuvo imágenes procesadas exitosamente")
            
        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"Error en procesamiento después de {total_time:.1f}s: {str(e)}")
            raise


def main():
    """Función principal para ejecutar el procesador"""
    
    # ========== CONFIGURACIÓN ==========
    MAX_EMPRESAS = 1  # Cambiar este número para procesar múltiples empresas
    # ===================================
    
    total_start_time = time.time()
    
    try:
        # Crear instancia del procesador
        processor = AIPhotoVertexProcessor()
        logger.info("=== PROCESAMIENTO DE EMPRESAS ===")
        logger.info(f"Configurado para procesar máximo {MAX_EMPRESAS} empresa(s)")

        empresas_procesadas = 0
        total_imagenes_procesadas = 0
        construction_images = 0
        successful_images = 0
        failed_images = 0
        execution_start = time.strftime('%Y-%m-%d %H:%M:%S')
        id_scraping_list = []

        # Procesar empresas según la configuración
        for empresa_num in range(1, MAX_EMPRESAS + 1):
            logger.info(f"\n--- Buscando empresa {empresa_num}/{MAX_EMPRESAS} ---")
            # Verificar si hay empresas pendientes
            id_scraping = processor.get_next_pending_id_scraping()
            if id_scraping is None:
                logger.info("No hay más empresas pendientes")
                break

            id_scraping_list.append(id_scraping)

            # Procesar la empresa completa
            empresa_start_time = time.time()
            images_before = processor.get_images_to_process(id_scraping=id_scraping)
            num_images = len(images_before)
            logger.info(f"Procesando empresa {empresa_num}: ID {id_scraping}")

            # Procesar imágenes y recolectar estadísticas
            images = images_before
            company_context = processor.get_company_context(id_scraping)
            for img_data in images:
                result = processor.process_single_image(img_data, company_context)
                total_imagenes_procesadas += 1
                if result is None:
                    failed_images += 1  # Timeout o error grave
                elif result is True:
                    successful_images += 1  # Procesada exitosamente
                    construction_images += 1  # Y es de construcción
                elif result is False:
                    successful_images += 1  # Procesada exitosamente pero no es de construcción
            empresas_procesadas += 1
            empresa_time = time.time() - empresa_start_time
            logger.info(f"Empresa {id_scraping} completada en {empresa_time:.1f}s")
            if empresa_num < MAX_EMPRESAS:
                logger.info("Pausa de 3 segundos antes de la siguiente empresa...")
                time.sleep(3)

        execution_end = time.strftime('%Y-%m-%d %H:%M:%S')
        total_time = time.time() - total_start_time

        logger.info("\n" + "="*50)
        logger.info("=== RESUMEN FINAL DE LA EJECUCIÓN ===")
        logger.info(f"Tiempo total de ejecución: {total_time:.1f}s ({total_time/60:.1f}min)")
        logger.info(f"Empresas procesadas: {empresas_procesadas}")
        logger.info(f"Total imágenes procesadas: {total_imagenes_procesadas}")
        logger.info(f"Imágenes de construcción: {construction_images}")
        logger.info(f"Imágenes exitosas: {successful_images}")
        logger.info(f"Imágenes fallidas: {failed_images}")

        if total_time > 0 and total_imagenes_procesadas > 0:
            avg_time_per_company = total_time / empresas_procesadas if empresas_procesadas > 0 else 0
            avg_time_per_image = total_time / total_imagenes_procesadas
            images_per_minute = (total_imagenes_procesadas * 60) / total_time
            logger.info(f"Tiempo promedio por empresa: {avg_time_per_company:.1f}s")
            logger.info(f"Tiempo promedio por imagen: {avg_time_per_image:.1f}s")
            logger.info(f"Velocidad global: {images_per_minute:.1f} img/min")

        # Insertar log en BigQuery
        try:
            # Generar log_id único INT64 usando timestamp en milisegundos
            log_id = int(time.time() * 1000)
            log_query = """
            INSERT INTO `web-scraping-468121.web_scraping_raw_data.vertex_photos_logs` (
                log_id, companies_processed, total_images_processed, construction_images, successful_images, failed_images, execution_start, execution_end, model_used
            ) VALUES (
                @log_id, @companies_processed, @total_images_processed, @construction_images, @successful_images, @failed_images, @execution_start, @execution_end, @model_used
            )
            """
            log_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("log_id", "INT64", log_id),
                    bigquery.ScalarQueryParameter("companies_processed", "INT64", empresas_procesadas),
                    bigquery.ScalarQueryParameter("total_images_processed", "INT64", total_imagenes_procesadas),
                    bigquery.ScalarQueryParameter("construction_images", "INT64", construction_images),
                    bigquery.ScalarQueryParameter("successful_images", "INT64", successful_images),
                    bigquery.ScalarQueryParameter("failed_images", "INT64", failed_images),
                    bigquery.ScalarQueryParameter("execution_start", "TIMESTAMP", execution_start),
                    bigquery.ScalarQueryParameter("execution_end", "TIMESTAMP", execution_end),
                    bigquery.ScalarQueryParameter("model_used", "STRING", processor.model_id),
                ]
            )
            processor.bq_client.query(log_query, job_config=log_job_config).result()
            logger.info(f"Log de ejecución insertado en vertex_photos_logs con log_id={log_id}.")
        except Exception as log_e:
            logger.error(f"Error insertando log de ejecución: {str(log_e)}")

        logger.info("=== PROCESAMIENTO COMPLETADO ===")
        logger.info("="*50)

    except Exception as e:
        total_time = time.time() - total_start_time
        logger.error(f"Error después de {total_time:.1f}s: {str(e)}")
        raise
    finally:
        input("\nPresiona Enter para continuar...")


if __name__ == "__main__":
    main()