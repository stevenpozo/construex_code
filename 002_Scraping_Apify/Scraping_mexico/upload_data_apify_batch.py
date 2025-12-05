"""Upload and process Apify batch data from apify_runs.txt to BigQuery."""

import os
import json
import re
import time
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from apify_client import ApifyClient
except Exception:
    raise SystemExit("This script requires the `apify-client` package. Install with: pip install apify-client")

try:
    from google.cloud import bigquery
    from google.cloud import storage
except Exception:
    raise SystemExit("This script requires the `google-cloud-bigquery` and `google-cloud-storage` packages. Install with: pip install google-cloud-bigquery google-cloud-storage")

try:
    import requests
except Exception:
    raise SystemExit("This script requires the `requests` package. Install with: pip install requests")

try:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
except Exception:
    raise SystemExit("This script requires threading support")

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def get_ecuador_timestamp() -> str:
    """Get current timestamp in Ecuador timezone."""
    if ZoneInfo is not None:
        tz = ZoneInfo("America/Guayaquil")
    else:
        tz = timezone(timedelta(hours=-5))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def parse_apify_runs_file(file_path: str) -> List[Dict[str, Any]]:
    """Parse apify_runs.txt to extract unprocessed batches."""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    batches = []
    blocks = content.split("-----------------------------------------")
    
    for i, block in enumerate(blocks):
        if not block.strip():
            continue
            
        lines = [line.strip() for line in block.strip().split("\n") if line.strip()]
        
        batch = {"processed": None, "photos_id": None, "page_id": None, "block_index": i, "block_content": block.strip()}
        
        for line in lines:
            if line.startswith("PROCESADO:"):
                batch["processed"] = line.split(":")[1].strip().upper() == "TRUE"
            elif "APIFY_ACTOR_PHOTOS" in line:
                # Next lines should contain id
                continue
            elif "APIFY_ACTOR_PAGE" in line:
                # Next lines should contain id
                continue
            elif line.startswith("- id ="):
                # Determine if this is photos or page based on context
                run_id = line.split("=")[1].strip()
                if batch["photos_id"] is None:
                    batch["photos_id"] = run_id
                else:
                    batch["page_id"] = run_id
        
        if batch["processed"] is False and batch["photos_id"] and batch["page_id"]:
            batches.append(batch)
    
    return batches


def download_apify_dataset_streaming(client: ApifyClient, run_id: str) -> List[Dict[str, Any]]:
    """Download dataset from Apify run with streaming support for large JSONs."""
    try:
        # Get run info to extract dataset ID
        run_info = client.run(run_id).get()
        dataset_id = run_info.get("defaultDatasetId")
        
        if not dataset_id:
            print(f"   ⚠️ No dataset ID found for run {run_id}")
            return []
        
        print(f"   📊 Dataset ID: {dataset_id}")
        
        # Get dataset items with proper iteration
        dataset_client = client.dataset(dataset_id)
        items = []
        
        # Use iterate_items for proper streaming support
        for item in dataset_client.iterate_items():
            items.append(item)
            
        return items
        
    except Exception as e:
        print(f"   ❌ Error downloading dataset for run {run_id}: {e}")
        return []


def process_page_data(page_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process APIFY_ACTOR_PAGE data and extract required fields."""
    processed = []
    
    for item in page_data:
        if not item.get("id_scraping"):
            continue
            
        processed_item = {
            "id_scraping": int(item["id_scraping"]),
            "facebookUrl": item.get("facebookUrl", ""),
            "address": item.get("address", ""),
            "category": item.get("category", ""),
            "email": item.get("email", ""),
            "intro": item.get("intro", ""),
            "phone": item.get("phone", ""),
            "title": item.get("title", ""),
            "profilePhoto": item.get("profilePictureUrl", ""),
            "coverPhoto": item.get("coverPhotoUrl", ""),
            "country": item.get("country", "Mexico").capitalize()  # Extract from userData and capitalize first letter
        }
        processed.append(processed_item)
    
    return processed


def process_photos_data(photos_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Process APIFY_ACTOR_PHOTOS data and group by facebookUrl."""
    grouped = {}
    
    for item in photos_data:
        facebook_url = item.get("facebookUrl", "")
        if not facebook_url:
            continue
            
        if facebook_url not in grouped:
            grouped[facebook_url] = []
            
        photo_item = {
            "image": item.get("image", ""),
            "facebookUrl": facebook_url
        }
        grouped[facebook_url].append(photo_item)
    
    return grouped


def create_temp_table_and_update_companies(bq_client: bigquery.Client, bq_table: str, 
                                         project_id: str, company_data: List[Dict[str, Any]]) -> int:
    """Create temporary table and MERGE to update company data in BQ_TABLE."""
    # Parse table components
    parts = bq_table.split(".")
    if len(parts) == 3:
        bq_project, bq_dataset, bq_table_name = parts
    elif len(parts) == 2:
        bq_project = project_id
        bq_dataset, bq_table_name = parts
    else:
        raise SystemExit("BQ_TABLE must be in format project.dataset.table or dataset.table")

    temp_table_name = f"company_updates_{int(time.time())}"
    temp_table_id = f"{bq_project}.{bq_dataset}.{temp_table_name}"

    # Create schema for temp table
    schema = [
        bigquery.SchemaField("id_scraping", "INT64"),
        bigquery.SchemaField("Address", "STRING"),
        bigquery.SchemaField("Category", "STRING"),
        bigquery.SchemaField("Email", "STRING"),
        bigquery.SchemaField("Intro", "STRING"),
        bigquery.SchemaField("Phone", "STRING"),
        bigquery.SchemaField("Title", "STRING"),
        bigquery.SchemaField("Created_at", "TIMESTAMP"),
    ]
    
    table_ref = bigquery.Table(temp_table_id, schema=schema)
    table_ref = bq_client.create_table(table_ref)

    # Prepare data for load
    created_at = get_ecuador_timestamp()
    rows_to_insert = []
    
    for item in company_data:
        row = {
            "id_scraping": item["id_scraping"],
            "Address": item["address"][:500] if item["address"] else "",  # Truncate if too long
            "Category": item["category"][:200] if item["category"] else "",
            "Email": item["email"][:100] if item["email"] else "",
            "Intro": item["intro"][:1000] if item["intro"] else "",
            "Phone": item["phone"][:50] if item["phone"] else "",
            "Title": item["title"][:300] if item["title"] else "",
            "Created_at": created_at
        }
        rows_to_insert.append(row)

    # Load data into temp table
    load_job = bq_client.load_table_from_json(rows_to_insert, table_ref)
    load_job.result()

    # MERGE query
    merge_query = f"""
    MERGE `{bq_project}.{bq_dataset}.{bq_table_name}` T
    USING `{temp_table_id}` S
    ON T.id_scraping = S.id_scraping
    WHEN MATCHED THEN UPDATE SET 
        Address = S.Address,
        Category = S.Category,
        Email = S.Email,
        Intro = S.Intro,
        Phone = S.Phone,
        Title = S.Title,
        Created_at = S.Created_at
    """

    merge_job = bq_client.query(merge_query)
    merge_job.result()
    affected = merge_job.num_dml_affected_rows

    # Clean up temp table
    bq_client.delete_table(table_ref)
    
    return affected


def generate_public_image_urls(bucket_name: str, company_data: List[Dict[str, Any]], 
                             photos_grouped: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Generate public URLs for images and prepare insert data for CLEANED_TABLE."""
    insert_data = []
    
    for company in company_data:
        id_scraping = company["id_scraping"]
        facebook_url = company["facebookUrl"]
        country = company.get("country", "Mexico").capitalize()  # Capitalize first letter
        
        has_images = False
        
        # Process profile photo
        if company.get("profilePhoto"):
            has_images = True
            object_name = f"{id_scraping}_profile_image"
            public_url = f"https://storage.googleapis.com/{bucket_name}/{object_name}"
            
            # Generate unique ID for this image record
            id_photo_cleaned = int(hashlib.md5(f"{id_scraping}_profile".encode()).hexdigest()[:8], 16)
            
            insert_data.append({
                "id_photo_cleaned": id_photo_cleaned,
                "id_scraping": id_scraping,
                "country": country,
                "img_path": public_url,
                "image_type": "profile_image",
                "created_at": get_ecuador_timestamp(),
                "original_url": company["profilePhoto"]
            })

        # Process cover photo
        if company.get("coverPhoto"):
            has_images = True
            object_name = f"{id_scraping}_cover_image"
            public_url = f"https://storage.googleapis.com/{bucket_name}/{object_name}"
            
            id_photo_cleaned = int(hashlib.md5(f"{id_scraping}_cover".encode()).hexdigest()[:8], 16)
            
            insert_data.append({
                "id_photo_cleaned": id_photo_cleaned,
                "id_scraping": id_scraping,
                "country": country,
                "img_path": public_url,
                "image_type": "cover_image",
                "created_at": get_ecuador_timestamp(),
                "original_url": company["coverPhoto"]
            })

        # Process post images from APIFY_ACTOR_PHOTOS
        if facebook_url in photos_grouped:
            photos = photos_grouped[facebook_url]
            for i, photo in enumerate(photos, 1):
                if photo.get("image"):
                    has_images = True
                    object_name = f"{id_scraping}_image{i}"
                    public_url = f"https://storage.googleapis.com/{bucket_name}/{object_name}"
                    
                    id_photo_cleaned = int(hashlib.md5(f"{id_scraping}_post_{i}".encode()).hexdigest()[:8], 16)
                    
                    insert_data.append({
                        "id_photo_cleaned": id_photo_cleaned,
                        "id_scraping": id_scraping,
                        "country": country,
                        "img_path": public_url,
                        "image_type": "post_image",
                        "created_at": get_ecuador_timestamp(),
                        "original_url": photo["image"]
                    })

        # Mark company as having images or not
        company["has_images"] = has_images
    
    return insert_data


def upload_single_image(storage_client, bucket_name: str, image_record: Dict[str, Any]) -> tuple:
    """Upload a single image to bucket. Returns (success: bool, error_msg: str)."""
    try:
        original_url = image_record.get("original_url")
        img_path = image_record.get("img_path", "")
        
        if not original_url or not img_path:
            return (False, "Missing URL or path")
            
        # Extract object name from public URL
        object_name = img_path.split(f"https://storage.googleapis.com/{bucket_name}/")[-1]
        
        # Download image from original URL
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(original_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Check if response has content
        if not response.content:
            return (False, "Empty image content")
        
        # Upload to bucket
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_string(response.content)
        
        # Set content type for images
        if original_url.lower().endswith(('.jpg', '.jpeg')):
            blob.content_type = 'image/jpeg'
        elif original_url.lower().endswith('.png'):
            blob.content_type = 'image/png'
        
        return (True, "")
        
    except Exception as e:
        return (False, str(e))


def upload_images_to_bucket(bucket_name: str, service_account: str, 
                          insert_data: List[Dict[str, Any]], companies_to_process: List[Dict[str, Any]]) -> int:
    """Download images from original URLs and upload to Google Cloud Storage bucket using threading."""
    if not insert_data:
        return 0
        
    # Initialize storage client
    storage_client = storage.Client.from_service_account_json(service_account)
    
    print(f"\n   ☁️ Subiendo imágenes al bucket por empresa:")
    
    # Group images by company for detailed logging
    images_by_company = {}
    for image in insert_data:
        id_scraping = image["id_scraping"]
        if id_scraping not in images_by_company:
            images_by_company[id_scraping] = []
        images_by_company[id_scraping].append(image)
    
    uploaded_count = 0
    failed_count = 0
    
    # Process each company's images
    for i, (id_scraping, company_images) in enumerate(images_by_company.items(), 1):
        total_companies = len(images_by_company)
        print(f"   [{i}/{total_companies}] 🏢 Empresa {id_scraping}: procesando {len(company_images)} imágenes...")
        
        company_uploaded = 0
        company_failed = 0
        error_details = []
        
        # Use ThreadPoolExecutor for parallel uploads of this company's images
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all upload tasks for this company
            future_to_record = {
                executor.submit(upload_single_image, storage_client, bucket_name, image): image 
                for image in company_images
            }
            
            # Collect results for this company
            for future in as_completed(future_to_record):
                success, error_msg = future.result()
                if success:
                    company_uploaded += 1
                else:
                    company_failed += 1
                    if error_msg:
                        error_details.append(error_msg)
        
        uploaded_count += company_uploaded
        failed_count += company_failed
        
        if company_failed > 0:
            print(f"   [{i}/{total_companies}] ⚠️ Empresa {id_scraping}: {company_uploaded} subidas, {company_failed} fallos")
            # Show first few error details
            for error in error_details[:3]:
                print(f"        🔍 Error: {error}")
        else:
            print(f"   [{i}/{total_companies}] ✅ Empresa {id_scraping}: {company_uploaded} subidas exitosas")
    
    print(f"   🎯 Total final: {uploaded_count} subidas exitosas, {failed_count} fallos")
    return uploaded_count


def insert_images_to_cleaned_table(bq_client: bigquery.Client, cleaned_table: str, 
                                 project_id: str, insert_data: List[Dict[str, Any]]) -> int:
    """Insert image records into CLEANED_TABLE using load job."""
    if not insert_data:
        return 0
        
    # Parse table components
    parts = cleaned_table.split(".")
    if len(parts) == 3:
        bq_project, bq_dataset, bq_table_name = parts
    elif len(parts) == 2:
        bq_project = project_id
        bq_dataset, bq_table_name = parts
    else:
        raise SystemExit("CLEANED_TABLE must be in format project.dataset.table or dataset.table")

    table_id = f"{bq_project}.{bq_dataset}.{bq_table_name}"
    
    # Prepare rows for insert (remove original_url as it's not in target table)
    rows_for_insert = []
    for row in insert_data:
        clean_row = {k: v for k, v in row.items() if k != "original_url"}
        rows_for_insert.append(clean_row)

    # Load data
    table = bq_client.get_table(table_id)
    load_job = bq_client.load_table_from_json(rows_for_insert, table)
    load_job.result()
    
    return len(rows_for_insert)


def update_batch_processed_status(file_path: str, batch: Dict[str, Any]) -> None:
    """Update PROCESADO to TRUE for the specific batch in apify_runs.txt."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Replace PROCESADO: FALSE with PROCESADO: TRUE in the specific batch
        updated_content = content.replace(
            batch["block_content"],
            batch["block_content"].replace("PROCESADO: FALSE", "PROCESADO: TRUE")
        )
        
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(updated_content)
            
        print(f"✓ Updated batch status to PROCESADO: TRUE in {file_path}")
        
    except Exception as e:
        print(f"⚠️ Error updating batch status: {e}")


def check_companies_already_processed(bq_client: bigquery.Client, bq_table: str, 
                                    project_id: str, id_scrapings: List[int]) -> Dict[int, str]:
    """Check which companies are already processed using Created_at as main flag."""
    if not id_scrapings:
        return {}
        
    ids_str = ",".join([str(id_val) for id_val in id_scrapings])
    
    query = f"""
    SELECT id_scraping, processed, is_downloaded, Created_at
    FROM `{bq_table}`
    WHERE id_scraping IN ({ids_str}) AND Created_at IS NOT NULL
    """
    
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        
        processed_status = {}
        for row in results:
            # Determine status based on the new logic
            if row["processed"] and row["is_downloaded"] and row["Created_at"]:
                status = "PROCESSED WITH IMAGES"
            elif not row["processed"] and row["is_downloaded"] and row["Created_at"]:
                status = "PROCESSED NO IMAGES"
            else:
                status = "UNKNOWN STATE"
            
            processed_status[row["id_scraping"]] = status
            
        return processed_status
        
    except Exception as e:
        print(f"⚠️ Error checking processed companies: {e}")
        return {}
def update_processed_status(bq_client: bigquery.Client, bq_table: str, project_id: str, 
                          company_data: List[Dict[str, Any]]) -> int:
    """Update processed status to true/false based on whether images were found."""
    if not company_data:
        return 0
        
    # Parse table components
    parts = bq_table.split(".")
    if len(parts) == 3:
        bq_project, bq_dataset, bq_table_name = parts
    elif len(parts) == 2:
        bq_project = project_id
        bq_dataset, bq_table_name = parts
    else:
        raise SystemExit("BQ_TABLE must be in format project.dataset.table or dataset.table")

    temp_table_name = f"processed_updates_{int(time.time())}"
    temp_table_id = f"{bq_project}.{bq_dataset}.{temp_table_name}"

    # Create schema for temp table
    schema = [
        bigquery.SchemaField("id_scraping", "INT64"),
        bigquery.SchemaField("processed", "BOOLEAN"),
    ]
    
    table_ref = bigquery.Table(temp_table_id, schema=schema)
    table_ref = bq_client.create_table(table_ref)

    # Prepare data for load
    rows_to_insert = []
    for company in company_data:
        rows_to_insert.append({
            "id_scraping": company["id_scraping"],
            "processed": company.get("has_images", False)
        })

    # Load data into temp table
    load_job = bq_client.load_table_from_json(rows_to_insert, table_ref)
    load_job.result()

    # MERGE query
    merge_query = f"""
    MERGE `{bq_project}.{bq_dataset}.{bq_table_name}` T
    USING `{temp_table_id}` S
    ON T.id_scraping = S.id_scraping
    WHEN MATCHED THEN UPDATE SET processed = S.processed
    """

    merge_job = bq_client.query(merge_query)
    merge_job.result()
    affected = merge_job.num_dml_affected_rows

    # Clean up temp table
    bq_client.delete_table(table_ref)
    
    return affected


def main():
    """Main function to process Apify batch data."""
    
    # Load config
    service_account = os.getenv("SERVICE_ACCOUNT_FILE")
    project_id = os.getenv("PROJECT_ID")
    bq_table = os.getenv("BQ_TABLE")
    cleaned_table = os.getenv("CLEANED_TABLE")
    bucket_name = os.getenv("BUCKET_NAME")
    apify_token = os.getenv("APIFY_TOKEN")

    if not all([service_account, project_id, bq_table, cleaned_table, bucket_name, apify_token]):
        raise SystemExit("❌ Missing required env vars")

    # Initialize clients
    bq_client = bigquery.Client.from_service_account_json(service_account, project=project_id)
    apify_client = ApifyClient(apify_token)

    # Parse apify runs file
    runs_file = "batch_apify/apify_runs.txt"
    if not os.path.exists(runs_file):
        print(f"❌ File not found: {runs_file}")
        return

    batches = parse_apify_runs_file(runs_file)
    if not batches:
        print("ℹ️ No unprocessed batches found")
        return

    print(f"Found {len(batches)} unprocessed batches.")

    for i, batch in enumerate(batches, 1):
        print(f"\n📦 BATCH #{i}/{len(batches)}")

        # Download datasets
        page_data = download_apify_dataset_streaming(apify_client, batch['page_id'])
        photos_data = download_apify_dataset_streaming(apify_client, batch['photos_id'])

        if not page_data:
            continue

        # Process data
        processed_companies = process_page_data(page_data)
        photos_grouped = process_photos_data(photos_data)

        if not processed_companies:
            continue

        # Check if companies are already processed
        id_scrapings = [company["id_scraping"] for company in processed_companies]
        already_processed = check_companies_already_processed(bq_client, bq_table, project_id, id_scrapings)
        
        # Filter out already processed companies
        companies_to_process = []
        for company in processed_companies:
            if company["id_scraping"] not in already_processed:
                companies_to_process.append(company)
        
        if not companies_to_process:
            update_batch_processed_status(runs_file, batch)
            continue

        print(f"\n   🔍 Procesando empresas individualmente:")
        companies_with_images = 0
        companies_without_images = 0
        
        for i, company in enumerate(companies_to_process, 1):
            id_scraping = company["id_scraping"]
            facebook_url = company["facebookUrl"]
            
            # Count images EXACTLY like generate_public_image_urls() does
            profile_image = 1 if company.get("profilePhoto") else 0
            cover_image = 1 if company.get("coverPhoto") else 0
            
            # Count post images using facebook_url (not id_scraping) like the real function
            post_images = 0
            if facebook_url in photos_grouped:
                photos = photos_grouped[facebook_url]
                for photo in photos:
                    if photo.get("image"):  # Only count photos that have image URL
                        post_images += 1
            
            total_company_images = profile_image + cover_image + post_images
            
            # A company has images if it has ANY image from ANY source
            has_any_images = (total_company_images > 0)
            
            if has_any_images:
                companies_with_images += 1
                company["has_images"] = True
                print(f"   [{i}/{len(companies_to_process)}] ✅ Empresa {id_scraping}: {total_company_images} imágenes encontradas (actor photos + actor page)")
            else:
                companies_without_images += 1
                company["has_images"] = False
                print(f"   [{i}/{len(companies_to_process)}] ❌ Empresa {id_scraping}: sin imágenes")
        
        print(f"\n   📊 Resumen:")
        print(f"   📊 Empresas encontradas: {len(companies_to_process)}")
        print(f"   ✅ Empresas CON imágenes: {companies_with_images}")
        print(f"   ❌ Empresas SIN imágenes: {companies_without_images}")

        # Generate image URLs
        insert_data = generate_public_image_urls(bucket_name, companies_to_process, photos_grouped)
        
        print(f"   📋 URLs generadas para subir: {len(insert_data)} registros")

        # Upload images to bucket
        uploaded_count = 0
        if insert_data:
            uploaded_count = upload_images_to_bucket(bucket_name, service_account, insert_data, companies_to_process)
        else:
            print(f"   ⚠️ No hay imágenes para subir al bucket")

        print(f"\n   📊 RESUMEN FINAL:")
        print(f"   ☁️ Imágenes subidas al bucket: {uploaded_count}/{len(insert_data) if insert_data else 0}")

        # Update database tables
        companies_with_images = [c for c in companies_to_process if c.get("has_images", False)]
        
        create_temp_table_and_update_companies(
            bq_client, bq_table, project_id, companies_to_process
        )
        
        if insert_data:
            insert_images_to_cleaned_table(
                bq_client, cleaned_table, project_id, insert_data
            )

        if companies_with_images:
            update_processed_status(bq_client, bq_table, project_id, companies_with_images)
        
        # Mark batch as processed
        update_batch_processed_status(runs_file, batch)

    print(f"\n🎉 Procesamiento completado: {len(batches)} batches")


if __name__ == "__main__":
    main()
