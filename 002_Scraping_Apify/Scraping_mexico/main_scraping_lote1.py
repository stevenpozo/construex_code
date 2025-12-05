

import requests
from google.cloud import storage
from google.cloud import bigquery
from apify_client import ApifyClient
import os
import pytz
from datetime import datetime
import time
from dotenv import load_dotenv

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv()

# === CONFIGURACIÓN DESDE .env ===
BUCKET_NAME = os.environ.get("BUCKET_NAME")
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE")
PROJECT_ID = os.environ.get("PROJECT_ID")
BQ_TABLE = os.environ.get("BQ_TABLE")
CLEANED_TABLE = os.environ.get("CLEANED_TABLE")
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
APIFY_ACTOR_PHOTOS = os.environ.get("APIFY_ACTOR_PHOTOS")
APIFY_ACTOR_PAGE = os.environ.get("APIFY_ACTOR_PAGE")

# === CLIENTES ===
client_gcs = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
bucket = client_gcs.bucket(BUCKET_NAME)
client_bq = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
apify_client = ApifyClient(APIFY_TOKEN)


# === CONTADOR DE TIEMPO ===
start_time = time.time()

# === CONSULTA: obtener lote pendiente ===
query = f"""
SELECT Link, id_scraping, Pais
FROM `{BQ_TABLE}`
WHERE is_downloaded = FALSE
LIMIT 1
"""

try:
    query_job = client_bq.query(query)
    rows = list(query_job)
    print(f"✅ Se encontraron {len(rows)} registros para procesar.")
except Exception as e:
    print(f"❌ Error al consultar BigQuery: {e}")
    rows = []

# === PROCESO POR CADA REGISTRO ===
for row in rows:
    link = row["Link"]
    id_scraping = row["id_scraping"]
    pais = str(row["Pais"]).capitalize() if row["Pais"] else ""
    print(f"\n🔹 Procesando id_scraping: {id_scraping} | País: {pais}")

    images_uploaded = 0
    try:
        # === Ejecutar actor de Apify ===
        run_input = {
            "startUrls": [{"url": link}],
            "resultsLimit": 10,
        }
        run = apify_client.actor(APIFY_ACTOR_PHOTOS).call(run_input=run_input)
        results = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        print(f"  - {len(results)} imágenes devueltas por Apify.")

        # === Eliminar duplicados ===
        unique_results = []
        seen = set()
        for item in results:
            img_url = item.get("image")
            if img_url and img_url not in seen:
                seen.add(img_url)
                unique_results.append(item)
        print(f"  - {len(unique_results)} imágenes únicas a procesar.")

        # === Subir imágenes únicas ===
        import random
        if not unique_results:
            print("    ⚠️ No se encontraron imágenes únicas para subir.")
        for idx, item in enumerate(unique_results):
            image_url = item.get("image")
            if not image_url:
                print(f"    ⚠️ Sin campo 'image' en resultado {idx}. No se insertará nada.")
                continue

            try:
                # Verificar si ya existe
                ext = os.path.splitext(image_url.split("?")[0])[1] or ".jpg"
                object_name = f"{id_scraping}_image{idx+1}{ext}"
                blob = bucket.blob(object_name)
                if blob.exists():
                    print(f"    ⚠️ Imagen {object_name} ya existe, saltando.")
                    continue

                # Descargar imagen
                response = requests.get(image_url, timeout=10)
                response.raise_for_status()

                # Subir a GCS
                blob.upload_from_string(
                    response.content,
                    content_type=response.headers.get("Content-Type", "image/jpeg")
                )
                img_path = f"https://storage.googleapis.com/{BUCKET_NAME}/{object_name}"

                # Generar id_photo_cleaned único (puedes usar timestamp + random)
                id_photo_cleaned = int(datetime.now().timestamp() * 1000) + random.randint(1, 999)

                # Insertar en tabla cleaned
                created_at = datetime.now(pytz.timezone("America/Guayaquil")).strftime("%Y-%m-%d %H:%M:%S")
                insert_query = f"""
                INSERT INTO `{CLEANED_TABLE}` (id_photo_cleaned, id_scraping, country, img_path, image_type, created_at)
                VALUES ({id_photo_cleaned}, {id_scraping}, '{pais}', '{img_path}', 'post_image', TIMESTAMP('{created_at}'))
                """
                client_bq.query(insert_query).result()
                images_uploaded += 1
                print(f"    ✅ Imagen subida y registrada: {object_name} | id_photo_cleaned: {id_photo_cleaned}")

            except Exception as e_img:
                print(f"    ❌ Error al subir/registrar imagen {image_url}: {e_img}")

        # === Nuevo actor Apify para cover y profile + datos extra ===
        try:
            alt_client = ApifyClient(APIFY_TOKEN)
            alt_run_input = {"startUrls": [{"url": link}]}
            alt_run = alt_client.actor(APIFY_ACTOR_PAGE).call(run_input=alt_run_input)
            alt_results = list(alt_client.dataset(alt_run["defaultDatasetId"]).iterate_items())
            if alt_results:
                alt_item = alt_results[0]  # Este actor devuelve un solo objeto por página
                # Extraer y limpiar los nuevos campos
                def clean_sql_string(s):
                    if not s:
                        return ""
                    return str(s).replace("'", "\\'").replace("\n", " ").replace("\r", " ")

                address = clean_sql_string(alt_item.get("address", ""))
                category = clean_sql_string(alt_item.get("category", ""))
                email = clean_sql_string(alt_item.get("email", ""))
                intro = clean_sql_string(alt_item.get("intro", ""))
                phone = clean_sql_string(alt_item.get("phone", ""))
                title = clean_sql_string(alt_item.get("title", ""))

                # Actualizar los nuevos campos en la tabla de links usando id_scraping
                update_fields_query = f"""
                UPDATE `{BQ_TABLE}`
                SET Address = '{address}', Category = '{category}', Email = '{email}', Intro = '{intro}', Phone = '{phone}', Title = '{title}', Created_at = TIMESTAMP('{created_at}')
                WHERE id_scraping = {id_scraping}
                """
                try:
                    client_bq.query(update_fields_query).result()
                    print(f"    📝 Campos extra actualizados en tabla raw para id_scraping {id_scraping}")
                except Exception as e_upd_fields:
                    print(f"    ❌ Error actualizando campos extra: {e_upd_fields}")

                found_image = False
                for key, img_type in [("coverPhotoUrl", "cover_image"), ("profilePictureUrl", "profile_image")]:
                    img_url = alt_item.get(key)
                    if not img_url:
                        print(f"    ⚠️ No se encontró {key} para id_scraping {id_scraping}. No se insertará nada.")
                        continue
                    try:
                        ext = os.path.splitext(img_url.split("?")[0])[1] or ".jpg"
                        object_name = f"{id_scraping}_{img_type}{ext}"
                        blob = bucket.blob(object_name)
                        if blob.exists():
                            print(f"    ⚠️ Imagen {object_name} ya existe, saltando.")
                            continue
                        response = requests.get(img_url, timeout=10)
                        response.raise_for_status()
                        blob.upload_from_string(
                            response.content,
                            content_type=response.headers.get("Content-Type", "image/jpeg")
                        )
                        img_path = f"https://storage.googleapis.com/{BUCKET_NAME}/{object_name}"
                        created_at = datetime.now(pytz.timezone("America/Guayaquil")).strftime("%Y-%m-%d %H:%M:%S")
                        id_photo_cleaned = int(datetime.now().timestamp() * 1000) + random.randint(1, 999)
                        insert_query = f"""
                        INSERT INTO `{CLEANED_TABLE}` (id_photo_cleaned, id_scraping, country, img_path, image_type, created_at)
                        VALUES ({id_photo_cleaned}, {id_scraping}, '{pais}', '{img_path}', '{img_type}', TIMESTAMP('{created_at}'))
                        """
                        client_bq.query(insert_query).result()
                        images_uploaded += 1
                        found_image = True
                        print(f"    ✅ Imagen subida y registrada: {object_name} | id_photo_cleaned: {id_photo_cleaned}")
                    except Exception as e_img2:
                        print(f"    ❌ Error al subir/registrar {img_type}: {e_img2}")
                if not found_image:
                    print(f"    ⚠️ No se insertó ninguna imagen cover/profile para id_scraping {id_scraping}.")
            else:
                print(f"    ⚠️ No se obtuvieron datos del actor alternativo para id_scraping {id_scraping}. No se insertará nada.")
        except Exception as e_alt:
            print(f"    ❌ Error ejecutando actor alternativo: {e_alt}")

    except Exception as e:
        print(f"  ❌ Error procesando id_scraping {id_scraping}: {e}")

    finally:
        # === Actualizar estado en BigQuery ===
        try:
            update_query = f"""
            UPDATE `{BQ_TABLE}`
            SET is_downloaded = TRUE
            WHERE id_scraping = {id_scraping}
            """
            client_bq.query(update_query).result()
            print(f"  - 🟩 id_scraping {id_scraping} marcado como descargado.")
        except Exception as e_upd:
            print(f"  ❌ Error actualizando is_downloaded: {e_upd}")

        if images_uploaded > 0:
            try:
                processed_query = f"""
                UPDATE `{BQ_TABLE}`
                SET processed = TRUE
                WHERE id_scraping = {id_scraping}
                """
                client_bq.query(processed_query).result()
                print(f"  - 🟦 id_scraping {id_scraping} marcado como processed ({images_uploaded} imágenes).")
            except Exception as e_proc:
                print(f"  ❌ Error actualizando processed: {e_proc}")

# === Mostrar tiempo de ejecución y pausa al final ===
end_time = time.time()
elapsed = end_time - start_time
mins, secs = divmod(int(elapsed), 60)
print(f"\nTiempo total de ejecución: {mins} min {secs} seg ({elapsed:.2f} segundos)")
input("\nProcesamiento finalizado. Presiona Enter para salir...")
