import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage

# -------- CONFIGURATION --------
BUCKET_NAME = "retail-data-pipeline"
API_URL = "https://fakerapi.it/api/v1/custom"
PARAMS = {
    "_quantity": 100,
    "order_id": "uuid",
    "customer": "name",
    "total": "price",
    "created_at": "date"
}
LOCAL_PATH = "./data/raw/"
os.makedirs(LOCAL_PATH, exist_ok=True)

# -------- STEP 1: FETCH DATA --------
def fetch_data():
    response = requests.get(API_URL, params=PARAMS)
    response.raise_for_status()
    data = response.json().get("data", [])
    return pd.DataFrame(data)

# -------- STEP 2: SAVE TO LOCAL FILE --------
def save_to_csv(df):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"orders_{ts}.csv"
    path = os.path.join(LOCAL_PATH, filename)
    df.to_csv(path, index=False)
    return path, filename

# -------- STEP 3: UPLOAD TO GCS --------
def upload_to_gcs(local_path, filename):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{filename}")
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded to GCS: gs://{BUCKET_NAME}/raw/{filename}")

# -------- MASTER FUNCTION --------
def run_ingestion():
    df = fetch_data()
    local_path, filename = save_to_csv(df)
    upload_to_gcs(local_path, filename)

# -------- TRIGGER SCRIPT --------
if __name__ == "__main__":
    run_ingestion()
