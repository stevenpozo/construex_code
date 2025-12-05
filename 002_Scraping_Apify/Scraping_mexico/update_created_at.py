import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

PROJECT_ID = os.getenv('PROJECT_ID')
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
CLEANED_TABLE = os.getenv('CLEANED_TABLE')
BQ_TABLE = os.getenv('BQ_TABLE')

# Inicializar cliente de BigQuery
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)


print("Iniciando proceso de actualización de fechas...")
# Consulta para obtener id_scraping y created_at de la tabla CLEANED_TABLE
query_get_dates = f"""
	SELECT id_scraping, created_at
	FROM `{CLEANED_TABLE}`
	WHERE created_at IS NOT NULL
"""
from datetime import datetime

# Fecha límite: hoy a las 10:00 am
fecha_limite = datetime(2025, 10, 27, 10, 0, 0)
fecha_limite_str = fecha_limite.strftime('%Y-%m-%d %H:%M:%S')

query_get_dates = f"""
		SELECT id_scraping, created_at
		FROM `{CLEANED_TABLE}`
			WHERE created_at IS NOT NULL
				AND created_at < TIMESTAMP('{fecha_limite_str}')
"""

print("Ejecutando consulta para obtener fechas...")
rows = client.query(query_get_dates).result()

# Crear diccionario id_scraping -> created_at (solo uno por id_scraping)
id_created_at = {}
total_rows = 0
for row in rows:
	total_rows += 1
	if row.id_scraping not in id_created_at:
		id_created_at[row.id_scraping] = row.created_at
		print(f"Fecha obtenida: {row.created_at} de id_scraping: {row.id_scraping} de la tabla cleaned")

print(f"Registros encontrados con created_at válido: {total_rows}")
print(f"IDs únicos a actualizar: {len(id_created_at)}")

actualizados = 0
for id_scraping, created_at in id_created_at.items():
	# Consultar el estado actual de Created_at en la tabla destino
	query_check = f"""
		SELECT Created_at FROM `{BQ_TABLE}` WHERE id_scraping = @id_scraping
	"""
	job_config_check = bigquery.QueryJobConfig(
		query_parameters=[
			bigquery.ScalarQueryParameter("id_scraping", "INT64", id_scraping),
		]
	)
	result_check = client.query(query_check, job_config=job_config_check).result()
	rows_check = list(result_check)
	if not rows_check:
		print(f"id_scraping {id_scraping} no existe en la tabla copy new.")
		continue
	estado_actual = rows_check[0].Created_at
	if estado_actual is not None:
		print(f"El campo Created_at para id_scraping {id_scraping} ya tiene valor: {estado_actual}. No se actualiza.")
		continue
	print(f"Insertando la fecha: {created_at} del id_scraping: {id_scraping} a la tabla copy new (Created_at estaba NULL)")
	query_update = f"""
		UPDATE `{BQ_TABLE}`
		SET Created_at = @created_at
		WHERE id_scraping = @id_scraping AND Created_at IS NULL
	"""
	job_config = bigquery.QueryJobConfig(
		query_parameters=[
			bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", created_at),
			bigquery.ScalarQueryParameter("id_scraping", "INT64", id_scraping),
		]
	)
	client.query(query_update, job_config=job_config)
	actualizados += 1

print(f"Proceso finalizado. Total de IDs procesados: {actualizados}")
input("Presiona ENTER para salir...")
