
**Resumen rápido — 002_Scraping_Apify**

**Estructura y contenido (muy breve)**


**Resumen por proyecto**

- `Scraping_brasil`
	- Qué hace: contiene dos implementaciones (`extract_data_new_webscraping.py` y `extract_data_old_webscraping.py`) para localizar empresas en Google Cloud Storage (GCS), comparar/empatar esos nombres con registros en una tabla BigQuery (`TABLA_1`), migrar registros nuevos a `TABLA_2` y procesar las imágenes relacionadas para almacenarlas en `TABLA_3`.
	- Puntos clave:
		- `new` vs `old`: ambos scripts hacen el mismo trabajo funcional pero apuntan a estructuras de carpetas distintas en GCS (nuevos scrapers vs antiguos).
		- Procesamiento de imágenes: detectan `Banner.jpg`, `Logo.jpg` y `Posts/` (post images); renombrado consistente usando `id_scraping`; subida al bucket destino y registro en BigQuery.
		- Uso de técnicas: normalización de nombres, comparación por similitud (SequenceMatcher), procesamiento paralelo (`ThreadPoolExecutor`) y protección contra duplicados en destino.

- `Scraping_mexico`
	- Qué hace: orquesta ejecuciones por lotes con Apify (`APIFY_ACTOR_PHOTOS`, `APIFY_ACTOR_PAGE`), descarga datasets, sube imágenes al bucket GCS, inserta registros en `CLEANED_TABLE` y enriquece la tabla `BQ_TABLE` con campos extra (address, category, title, etc.). Incluye además un módulo que usa Vertex AI para analizar imágenes (clasificarlas como construcción/no construcción y extraer productos).
	- Puntos clave:
		- Pipeline por lotes: `apify_batch.py` lanza actores, registra runs en `batch_apify/apify_runs.txt`; `upload_data_apify_batch.py` procesa esos runs (descarga datasets, agrupa fotos, sube imágenes y actualiza BQ).
		- Procesamiento inmediato: `main_scraping_lote1.py` procesa un link (LIMIT 1), descarga y sube imágenes y actualiza tablas.
		- Clasificación IA: `ai_photo_vertex_V8.py` consume `CLEANED_TABLE` y usa Vertex AI para extraer productos relevantes y actualizar metadata en BigQuery.

**Variables y credenciales**
- Ambos proyectos usan variables definidas en un `.env` local y JSON de service accounts dentro de una carpeta `Service/`. Ejemplos de variables que debes definir en cada subcarpeta:`SERVICE_ACCOUNT_FILE`, `PROJECT_ID`, `BUCKET_NAME`, `BQ_TABLE`, `CLEANED_TABLE`, `APIFY_TOKEN`, `APIFY_ACTOR_PHOTOS`, `APIFY_ACTOR_PAGE`, `SERVICE_ACCOUNT_FILE_VERTEX`, `MODEL_ID`, `LOCATION`, `MAX_EMPRESAS`.

**Seguridad y buenas prácticas**
- NO subir: `Service/*.json` ni el fichero `.env`. Asegúrate de que `.gitignore` contiene reglas para ignorar `Service/*.json` y `.env` (ya está configurado en el repo).





