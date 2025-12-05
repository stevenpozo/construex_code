"""Apify batch runner using BigQuery to fetch links and update status."""

import os
import json
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
try:
	from zoneinfo import ZoneInfo
except Exception:
	ZoneInfo = None

try:
	from apify_client import ApifyClient
except Exception:
	raise SystemExit("This script requires the `apify-client` package. Install with: pip install apify-client")

try:
	from dotenv import load_dotenv
	load_dotenv()
except Exception:
	pass

try:
	from google.cloud import bigquery
except Exception:
	raise SystemExit("This script requires the `google-cloud-bigquery` package. Install with: pip install google-cloud-bigquery")


def main():
	# Load config from environment / .env
	service_account = os.getenv("SERVICE_ACCOUNT_FILE")
	project_id = os.getenv("PROJECT_ID")
	bq_table = os.getenv("BQ_TABLE")
	max_empresas = int(os.getenv("MAX_EMPRESAS", "10"))

	apify_token = os.getenv("APIFY_TOKEN")
	actor_photos = os.getenv("APIFY_ACTOR_PHOTOS")
	actor_page = os.getenv("APIFY_ACTOR_PAGE")

	if not all([service_account, project_id, bq_table, apify_token, actor_photos, actor_page]):
		raise SystemExit("Missing one of required env vars: SERVICE_ACCOUNT_FILE, PROJECT_ID, BQ_TABLE, APIFY_TOKEN, APIFY_ACTOR_PHOTOS, APIFY_ACTOR_PAGE")

	# BigQuery client
	bq_client = bigquery.Client.from_service_account_json(service_account, project=project_id)

	# Query to fetch links and id_scraping
	query = f"""
	SELECT distinct(Link), id_scraping, Pais
	FROM `{bq_table}`
	WHERE (processed = false and is_downloaded = false and Created_at is null)
	LIMIT {max_empresas}
	"""

	print("Executing BigQuery to fetch links...")
	query_job = bq_client.query(query)
	rows = list(query_job.result())

	if not rows:
		print("No records to process.")
		return

	links = [r["Link"] for r in rows]
	id_scrapings = [r["id_scraping"] for r in rows]

	print(f"Records fetched: {len(rows)}")

	# Start Apify actors
	client = ApifyClient(apify_token)

	def start_actor_with_userdata(actor_id: str, items: List[Dict[str, str]], extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
		# items: list of {'url': ..., 'id_scraping': ..., 'country': ...}
		run_input: Dict[str, Any] = {"startUrls": [{"url": it["url"], "userData": {"id_scraping": it["id_scraping"], "country": it["country"]}} for it in items]}
		if extra:
			# merge extra top-level parameters into run_input (non-destructive)
			for k, v in extra.items():
				if k not in run_input:
					run_input[k] = v
		run_info = client.actor(actor_id).start(run_input=run_input)
		run_id = run_info.get("id") or run_info.get("data", {}).get("id")
		return {"id": run_id, "monitor": f"https://my.apify.com/actors/{actor_id}/runs/{run_id}", "run_info": run_info}

	items = [{"url": r["Link"], "id_scraping": str(r["id_scraping"]), "country": r["Pais"]} for r in rows]

	print("Launching APIFY_ACTOR_PHOTOS (limit 10 images per link)...")
	# Actor expects `resultsLimit` for photos limit
	photos_run = start_actor_with_userdata(actor_photos, items, extra={"resultsLimit": 10})
	print("Launching APIFY_ACTOR_PAGE...")
	page_run = start_actor_with_userdata(actor_page, items)

	# BigQuery: create temporary table with ids and MERGE to update is_downloaded = TRUE
	# Parse project.dataset.table from bq_table
	parts = bq_table.split(".")
	if len(parts) == 3:
		bq_project, bq_dataset, bq_table_name = parts
	elif len(parts) == 2:
		bq_project = project_id
		bq_dataset, bq_table_name = parts
	else:
		raise SystemExit("BQ_TABLE must be in format project.dataset.table or dataset.table")

	import time
	temp_table_name = f"apify_temp_ids_{int(time.time())}"
	temp_table_id = f"{bq_project}.{bq_dataset}.{temp_table_name}"

	schema = [bigquery.SchemaField("id_scraping", "INT64")]
	table_ref = bigquery.Table(temp_table_id, schema=schema)
	table_ref = bq_client.create_table(table_ref)

	# Load ids into temp table as integers (match original table type INT)
	unique_ids = list({int(r["id_scraping"]) for r in rows})
	rows_to_insert = [{"id_scraping": uid} for uid in unique_ids]
	load_job = bq_client.load_table_from_json(rows_to_insert, table_ref)
	load_job.result()

	merge_query = f"""
	MERGE `{bq_project}.{bq_dataset}.{bq_table_name}` T
	USING `{temp_table_id}` S
	ON T.id_scraping = S.id_scraping
	WHEN MATCHED THEN UPDATE SET is_downloaded = TRUE
	"""

	print("Updating BigQuery flags (is_downloaded) via MERGE using temp table...")
	merge_job = bq_client.query(merge_query)
	merge_job.result()
	affected = merge_job.num_dml_affected_rows

	# Clean up temp table
	bq_client.delete_table(table_ref)

	# Write runs to batch file inside `batch_apify` directory, append entry
	batch_dir = "batch_apify"
	try:
		os.makedirs(batch_dir, exist_ok=True)
	except Exception:
		pass

	out_path = os.path.join(batch_dir, "apify_runs.txt")

	# Ecuador timezone (America/Guayaquil). Fallback to fixed -5 UTC if zoneinfo missing.
	if ZoneInfo is not None:
		tz = ZoneInfo("America/Guayaquil")
	else:
		tz = timezone(timedelta(hours=-5))

	now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

	description = f"DESCRIPCIÓN : Se lanzaron {len(rows)} empresas a scrapear"
	entry_lines = []
	entry_lines.append(description)
	entry_lines.append(f"FECHA: {now}")
	entry_lines.append("PROCESADO: FALSE")
	entry_lines.append("")
	entry_lines.append("ACTORES USADOS:")
	entry_lines.append("- APIFY_ACTOR_PHOTOS")
	entry_lines.append(f"    - id = {photos_run.get('id')}")
	entry_lines.append(f"    - enlace = {photos_run.get('monitor')}")
	entry_lines.append("")
	entry_lines.append("- APIFY_ACTOR_PAGE")
	entry_lines.append(f"    - id = {page_run.get('id')}")
	entry_lines.append(f"    - enlace = {page_run.get('monitor')}")
	entry_lines.append("")
	entry_lines.append("-----------------------------------------")
	entry_lines.append("")

	with open(out_path, "a", encoding="utf-8") as f:
		f.write("\n".join(entry_lines) + "\n")

	# Logs summary
	print("--- Summary ---")
	print(f"Companies fetched: {len(rows)}")
	print(f"Companies marked is_downloaded = TRUE: {affected}")
	print("Runs:")
	print(f"APIFY_ACTOR_PHOTOS - id: {photos_run['id']} - monitor: {photos_run['monitor']}")
	print(f"APIFY_ACTOR_PAGE   - id: {page_run['id']} - monitor: {page_run['monitor']}")
	print(f"Run details saved to: {out_path}")


if __name__ == "__main__":
	main()

