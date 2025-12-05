
**Proyecto: Scraping_mexico**
- Propósito: pipelines y utilidades para ejecutar scrapers (Apify), subir imágenes a Google Cloud Storage (GCS) y sincronizar datos con BigQuery. Los scripts utilizan credenciales locales almacenadas en `Service/` y variables de configuración en `.env` (ambos no versionados).

**Archivos y descripción detallada**

**`ai_photo_vertex_V8.py`**
- **Función**: Analizar imágenes almacenadas en `CLEANED_TABLE` usando Vertex AI (Gemini) para detectar si la imagen contiene productos del sector construcción y extraer metadatos/productos en formato JSON.
- **Flujo**:
	- Carga variables desde `.env` (`SERVICE_ACCOUNT_FILE`, `SERVICE_ACCOUNT_FILE_VERTEX`, `PROJECT_ID`, `CLEANED_TABLE`, `VERTEX_PROJECT_ID`, `LOCATION`, `MODEL_ID`).
	- Inicializa credenciales y clientes: BigQuery/Storage con `SERVICE_ACCOUNT_FILE` y Vertex con `SERVICE_ACCOUNT_FILE_VERTEX`.
	- Consulta la tabla `CLEANED_TABLE` para imágenes `post_image` pendientes (`is_construction_image IS NULL`) y procesa en lotes.
	- Por cada imagen: crea un `Part` con la URI en GCS y envía prompt+imagen al modelo Vertex configurado para devolver un esquema JSON con `products`.
	- Manejo robusto: timeouts (hilo por imagen), extracción de metadata de tokens, reintentos/errores controlados.
	- Actualiza la fila en BigQuery con campos: `is_construction_image`, `product_information`, `token_input`, `token_output`, `model_used`, `execution_time_seconds`, `processed_ia_at`.
	- Inserta un log resumen en `vertex_photos_logs` al finalizar.
- **Dependencias de `Service/` y `.env`**: requiere `Service/web-scraping-468121-9c845a21c06b.json` (BigQuery/Storage) y `Service/categorizacion-2-emails-b2f50bb184de.json` (Vertex credentials) según `SERVICE_ACCOUNT_FILE` y `SERVICE_ACCOUNT_FILE_VERTEX` en `.env`.

**`apify_batch.py`**
- **Función**: Ejecuta en batch actores de Apify para fotos y página, marcando registros en BigQuery como `is_downloaded = TRUE` y guardando detalles de ejecución en `batch_apify/apify_runs.txt`.
- **Flujo**:
	- Lee `.env` (`SERVICE_ACCOUNT_FILE`, `PROJECT_ID`, `BQ_TABLE`, `MAX_EMPRESAS`, `APIFY_TOKEN`, `APIFY_ACTOR_PHOTOS`, `APIFY_ACTOR_PAGE`).
	- Consulta `BQ_TABLE` por links pendientes y lanza `APIFY_ACTOR_PHOTOS` y `APIFY_ACTOR_PAGE` con `userData` que incluye `id_scraping` y `country`.
	- Crea una tabla temporal con `id_scraping` y ejecuta un `MERGE` para actualizar `is_downloaded = TRUE` en la tabla principal.
	- Registra meta información del run en `batch_apify/apify_runs.txt` para su posterior procesado por `upload_data_apify_batch.py`.
- **Dependencias de `Service/` y `.env`**: usa `SERVICE_ACCOUNT_FILE` para autenticarse con BigQuery y `APIFY_TOKEN` desde `.env`.

**`main_scraping_lote1.py`**
- **Función**: Script simple que obtiene un link pendiente (`LIMIT 1`), ejecuta los actores de Apify, descarga las imágenes retornadas, las sube a `BUCKET_NAME` y registra cada imagen en `CLEANED_TABLE`. También actualiza datos adicionales (address, category, email, intro, phone, title) desde el actor de página.
- **Flujo**:
	- Carga variables desde `.env` (`BUCKET_NAME`, `SERVICE_ACCOUNT_FILE`, `PROJECT_ID`, `BQ_TABLE`, `CLEANED_TABLE`, `APIFY_TOKEN`, `APIFY_ACTOR_PHOTOS`, `APIFY_ACTOR_PAGE`).
	- Lanza el actor de fotos, deduplica URLs y para cada imagen única: descarga `requests.get`, sube al bucket con `blob.upload_from_string`, genera `id_photo_cleaned` y hace `INSERT` a `CLEANED_TABLE`.
	- Lanza actor página para extraer `coverPhotoUrl` y `profilePictureUrl`, subirlas y registrar también.
	- Marca `is_downloaded = TRUE` y `processed = TRUE` según corresponda en `BQ_TABLE`.
- **Dependencias de `Service/` y `.env`**: `SERVICE_ACCOUNT_FILE` para Storage y BigQuery; `BUCKET_NAME` y tablas en `.env`.

**`update_created_at.py`**
- **Función**: Sincroniza `Created_at` en la tabla `BQ_TABLE` tomando la fecha de `created_at` desde `CLEANED_TABLE` cuando la columna destino está `NULL`.
- **Flujo**:
	- Carga variables `.env` (`PROJECT_ID`, `SERVICE_ACCOUNT_FILE`, `CLEANED_TABLE`, `BQ_TABLE`).
	- Extrae `id_scraping` y `created_at` de `CLEANED_TABLE` y actualiza `BQ_TABLE` con `Created_at` si está NULL.
- **Notas**: actualmente la `fecha_limite` está hardcodeada en el script; se recomienda parametrizarla.

**`upload_data_apify_batch.py`**
- **Función**: Procesar batches listados en `batch_apify/apify_runs.txt`: descargar datasets de Apify, generar URLs públicas para images (destino en GCS), descargar/subir imágenes, insertar filas en `CLEANED_TABLE` y actualizar `BQ_TABLE` con datos de empresa.
- **Flujo**:
	- Lee `.env` (`SERVICE_ACCOUNT_FILE`, `PROJECT_ID`, `BQ_TABLE`, `CLEANED_TABLE`, `BUCKET_NAME`, `APIFY_TOKEN`).
	- Parsea `apify_runs.txt` para obtener `photos_id` y `page_id` por batch.
	- Descarga datasets (streaming), procesa `page_data` y `photos_data`, agrupa fotos por `facebookUrl`.
	- Genera `insert_data` con rutas públicas `https://storage.googleapis.com/{BUCKET_NAME}/{object_name}` y `id_photo_cleaned` (hash-based) y sube imágenes con `upload_images_to_bucket` (usa `ThreadPoolExecutor`).
	- Inserta registros a `CLEANED_TABLE` con `load_table_from_json` y actualiza `BQ_TABLE` mediante tabla temporal y `MERGE`.
	- Marca batch como `PROCESADO: TRUE` en `apify_runs.txt`.
- **Dependencias de `Service/` y `.env`**: `SERVICE_ACCOUNT_FILE` y `APIFY_TOKEN`, `BUCKET_NAME`, `CLEANED_TABLE`, `BQ_TABLE`.

**Relación general entre scripts, `Service/` y `.env`**
- **`Service/`**: carpeta local que contiene los JSON de credenciales (ej: `web-scraping-468121-9c845a21c06b.json`, `categorizacion-2-emails-b2f50bb184de.json`). Estos archivos son requeridos por los scripts para inicializar clientes de Google Cloud y Vertex AI.
- **`.env`**: centraliza rutas a estos JSON y parámetros operativos. Ejemplo de variables usadas por estos scripts (rellenar en `Scraping_mexico/.env`):

```
SERVICE_ACCOUNT_FILE=Service/web-scraping-468121-9c845a21c06b.json
SERVICE_ACCOUNT_FILE_VERTEX=Service/categorizacion-2-emails-b2f50bb184de.json
PROJECT_ID=web-scraping-468121
BUCKET_NAME=images_test_bucket
BQ_TABLE=web-scraping-468121.web_scraping_raw_data.mx_web_scraping_raw_update_copy_test
CLEANED_TABLE=web-scraping-468121.web_scraping_raw_data.mx_web_scraping_images_cleaned_test
APIFY_TOKEN=apify_api_...
APIFY_ACTOR_PHOTOS=sZZXWhobbj7dZpHhg
APIFY_ACTOR_PAGE=4Hv5RhChiaDk6iwad
VERTEX_PROJECT_ID=categorizacion-2-emails
MODEL_ID=gemini-2.5-flash
LOCATION=global
MAX_EMPRESAS=10
```

- **Seguridad**: ambos (`Service/*.json` y `.env`) NO deben subirse al repositorio. Añade `Service/*.json` y `.env` a `.gitignore` (ya existen reglas en el repo). Si algún JSON ya está versionado, usa `git rm --cached <ruta>` para eliminar del índice sin borrar localmente.



