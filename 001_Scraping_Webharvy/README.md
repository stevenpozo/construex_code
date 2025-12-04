**Resumen**
- **Descripción:** Breve orquestador para scraping de enlaces de Facebook usando BigQuery y WebHarvy. El sistema descarga lotes de links, los procesa con WebHarvy, gestiona imágenes y sube resultados a BigQuery.

**Estructura general**
- **Carpeta:** `001_Links_Facebook` — Almacena lotes de links y su historial (`001_Pending_Links`, `002_Processed_Links`).
- **Carpeta:** `002_Data` — Contiene los CSV resultantes y las carpetas con las fotos descargadas (`001_Data_CSV`, `002_Photos_Facebook`).
- **Carpeta:** `003_Service_Google_cloud` — Credenciales y archivos relacionados con el servicio de Google Cloud (p. ej. JSON de la cuenta de servicio).
- **Carpeta:** `004_Code` — Scripts de control y automatización que ejecutan el flujo completo.

**Archivos y scripts clave**
- **`Extract_links.py`**: Extrae enlaces desde BigQuery y crea lotes de texto para procesar.
- **`automatiza_webharvy.py`**: Lanza y controla WebHarvy para scrapear las páginas a partir de los lotes de links.
- **`load_image_buket.py`**: Procesa y sube imágenes (gestiona carpetas de fotos y movimiento de archivos).
- **`upload_data_bd.py`**: Sube los datos procesados (CSV/metadatos) a BigQuery.
- **`main.py`**: Orquestador principal que ejecuta los pasos anteriores en ciclos periódicos y maneja timeouts/errores básicos.
- **`.env`**: Variables de entorno usadas por los scripts (rutas, credenciales, etc.).
- **`webharvy_pid.txt`**: Archivo auxiliar para almacenar el PID de WebHarvy (uso para cierres forzados si hay timeout).

**Flujo de ejecución (alto nivel)**
- 1) `Extract_links.py` obtiene enlaces desde BigQuery y genera lotes en `001_Pending_Links`.
- 2) `automatiza_webharvy.py` procesa esos lotes con WebHarvy y genera un CSV de salida.
- 3) El orquestador (`main.py`) espera al CSV y valida que se hayan creado carpetas de imágenes.
- 4) `load_image_buket.py` sube/organiza las imágenes extraídas.
- 5) `upload_data_bd.py` envía los datos finales a BigQuery.
- 6) El ciclo se repite continuamente hasta que el usuario lo detenga (Ctrl+C).

**Cómo ejecutar (rápido)**
- Colocar las variables en `004_Code/.env` 
- Añadir la cuenta de servicio dentro de `003_Service_Google_cloud`
- y ejecutar el orquestador

	`python main.py`

**Notas útiles**
- El proceso es cíclico y diseñado para correr continuamente con pausas entre ciclos.
- Revisar `webharvy_pid.txt` si es necesario forzar el cierre de WebHarvy por timeout.
- Mantener actualizadas las credenciales de `003_Service_Google_cloud` y las rutas en `.env`.

---