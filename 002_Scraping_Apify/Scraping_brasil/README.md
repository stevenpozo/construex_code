
**Proyecto**
- `Scraping_brasil`: Conjunto de utilidades para procesar y migrar datos extraídos por scrapers (Apify) hacia Google Cloud (GCS / BigQuery) y para manejar imágenes asociadas.

**Archivos principales**
- `extract_data_new_webscraping.py`: Versión más reciente del proceso de migración.
	- Objetivo: leer nombres/entradas de empresas desde Google Cloud Storage (GCS), buscar registros coincidentes en la `TABLA_1` de BigQuery, y migrar los registros nuevos a `TABLA_2`. Además procesa imágenes asociadas y las inserta en `TABLA_3`.
	- Flujo general: carga variables desde `.env` (uso de `dotenv`), inicializa clientes de `google.cloud.storage` y `google.cloud.bigquery`, crea índices de nombres normalizados, realiza matching (por similitud de cadenas), filtra duplicados en `TABLA_2`, migra datos y procesa/almacena imágenes en GCS y BigQuery.
	- Notas: usa logging con archivo `migration_process.log`, multi-threading para matching y procesamiento de imágenes, y un umbral de similitud ajustable para encontrar coincidencias.

- `extract_data_old_webscraping.py`: Implementación anterior/conservadora del mismo proceso.
	- Objetivo: cumple la misma finalidad general que la versión nueva pero con diferencias en la estrategia de matching, tamaño de lotes y manejo de imágenes. Mantiene funciones equivalentes: lectura desde GCS, consulta de `TABLA_1`, matching, filtrado e inserción en `TABLA_2` y `TABLA_3`.
	- Uso: puede ser útil para comparar resultados históricos o cuando se prefiera la lógica anterior por estabilidad.

	Detalle importante sobre diferencias (resumen):
	- `extract_data_new_webscraping.py` extrae imágenes de la estructura de GCS creada por los scrapers nuevos. En el código se usa el prefijo:
		- `Webscraping/Paises/New Web Scraping/Brasil/` (cada entrada es un prefijo/nombre de empresa dentro de esa carpeta).
	- `extract_data_old_webscraping.py` procesa empresas que fueron scrapeadas anteriormente y por tanto tiene una estructura diferente en GCS. En el código se usa un prefijo tipo:
		- `Webscraping/Paises/Old Web Scraping/{FOLDER}/Posts/` (donde `{FOLDER}` viene de la variable `FOLDER_BRASIL1` definida en `.env`).

	Ambos scripts hacen esencialmente lo mismo en cuanto a imágenes, con diferencias en la ruta origen:
	- Buscan artefactos comunes por empresa: `Banner.jpg` (cover), `Logo.jpg` (profile) y las imágenes dentro de `Posts/`.
	- Comportamiento general de procesamiento de imágenes:
		- Detectan si existen blobs en el bucket origen (función `blob_exists` y listados con `list_blobs`).
		- Construyen una lista de imágenes encontradas con tupla `(ruta_origen, nuevo_nombre, tipo)` donde `nuevo_nombre` suele formarse usando `id_scraping` y un sufijo semántico (por ejemplo `12345_cover_image.jpg`, `12345_profile_image.jpg`, `12345_post_1.jpg`).
		- Copian/renombran las imágenes al bucket destino (`BUCKET_NAME`) con el nuevo nombre.
		- Si el bucket destino tiene `Uniform Bucket-Level Access` habilitado, no se configuran ACLs por objeto (los scripts lo detectan y emiten un log informativo). En caso contrario, pueden aplicar ACLs o dejar objetos con permisos por defecto.
		- Generan metadatos/filas para la `TABLA_3` (imagenes) con campos como `img_path`, `image_type`, `id_scraping`, `country`, `created_at`, etc., y las insertan en BigQuery en bloque usando `load_table_from_json`.
		- Emplean `ThreadPoolExecutor` para paralelizar el mapeo y procesamiento de imágenes por empresa, lo que acelera la descarga/renombrado/subida cuando hay muchas empresas.

	Efectos prácticos:
	- Resultado final: las imágenes quedan centralizadas en el bucket destino (`BUCKET_NAME`) con nombres normalizados/consistentes, y `TABLA_3` contiene las entradas con `img_path` apuntando a las rutas públicas/privadas según la configuración del bucket.


**Relación entre los scripts y `Service/`**
- Ambos scripts dependen de credenciales y recursos locales que se guardan en la carpeta `Service/` (no versionada):
	- `web-scraping-468121-9c845a21c06b.json`: archivo de cuenta de servicio de Google Cloud (service account). Se utiliza para inicializar los clientes de GCS y BigQuery (mediante `storage.Client.from_service_account_json(...)` y `bigquery.Client.from_service_account_json(...)`).
	- `client_secret_gmail.json`: credenciales OAuth para uso con Gmail API (si en algún proceso se envían correos o se autentica con OAuth). No es necesario para la migración de BigQuery/GCS, pero lo mantenemos en `Service/` porque el repo contiene utilidades que a veces usan Gmail.
	- `categorizacion-2-emails-b2f50bb184de.json`: otro JSON de servicio/credenciales (puede ser otro service account o credencial de API usada por alguna tarea de categorización). Mantener local por seguridad.
	- `leer.txt`: archivo de ayuda/nota local.

	Importante: esos JSON contienen secretos. No deben subirse al repositorio. La carpeta `Service/` ya está excluida en `.gitignore`.

**Archivo `.env` (local, NO subir)**
 - Ruta recomendada: colocar el archivo en `Scraping_brasil/.env` (junto a los scripts). Los scripts ya llaman a `load_dotenv()` para cargar estas variables.
 - Variables esperadas (ejemplo):

```
BIGQUERY_STORAGE_SERVICE=Service/web-scraping-468121-9c845a21c06b.json
BIGQUERY_STORAGE_ID=web-scraping-468121
BUCKET_NAME=brasil-data-clean
BUCKET_DRIVE_NAME=webscraping-drive
MAX_EMPRESAS=30
TABLA_1=web-scraping-468121.web_scraping_raw_data.br_web_scraping_raw_update_new
TABLA_2=web-scraping-468121.web_scraping_raw_data.br_web_scraping_raw_update_copy_new
TABLA_3=web-scraping-468121.web_scraping_raw_data.br_web_scraping_images_cleaned
FOLDER_BRASIL1=Brasil2
```

 - Consejos:
	 - `BIGQUERY_STORAGE_SERVICE` debe apuntar al JSON dentro de la carpeta `Service/` (o a la ruta absoluta si lo prefieres).
	 - No subir `.env` al control de versiones. El repo ya incluye una regla para ignorarlo en `/001_Scraping_Webharvy/004_Code/.env` y la carpeta `Services/` está en la raíz de `.gitignore`.
	 - Si prefieres no usar `.env`, exporta las variables en la sesión de PowerShell antes de ejecutar los scripts.

Ejecución sin entorno virtual

Si no usas un entorno virtual (paso aclarado):

- Instala las dependencias globalmente o usa el interprete Python ya disponible en tu sistema. Asegúrate de tener las librerías necesarias instaladas (`google-cloud-storage`, `google-cloud-bigquery`, `python-dotenv`, `pytz`, etc.).

- Ejecutar los scripts desde la carpeta `Scraping_brasil` (tras configurar `.env` o exportar variables):

```powershell
python extract_data_new_webscraping.py
python extract_data_old_webscraping.py
```

- Alternativa (sin `.env`): setear variables en la sesión de PowerShell:

```powershell
Set-Item -Path Env:BIGQUERY_STORAGE_SERVICE -Value 'Service/web-scraping-468121-9c845a21c06b.json'
Set-Item -Path Env:BIGQUERY_STORAGE_ID -Value 'web-scraping-468121'
Set-Item -Path Env:BUCKET_NAME -Value 'brasil-data-clean'
# ... repetir para las demás variables ...
python extract_data_new_webscraping.py
```

**`.gitignore` y seguridad**
- El repositorio ya incluye las reglas para evitar subir credenciales sensibles:
	- `Services/` está en `.gitignore` (ignora la carpeta que contiene los JSONs de credenciales).
	- La ruta `/001_Scraping_Webharvy/003_Service_Google_cloud/web-scraping-468121-9c845a21c06b.json` también está ignorada explícitamente.
	- `001_Scraping_Webharvy/004_Code/.env` está ignorado para evitar subir variables locales.

Verifica siempre antes de commitear con `git status` y `git diff --staged` para asegurarte de que no se están incluyendo archivos sensibles.




