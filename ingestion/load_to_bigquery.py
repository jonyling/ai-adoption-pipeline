import os
import logging
import pandas as pd

from dotenv import load_dotenv
from tqdm import tqdm

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


# -------------------------------------------------
# Logging configuration
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT_ID")
RAW_DATASET = os.getenv("BQ_RAW_DATASET")
CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
DATA_DIR = os.getenv("DATA_DIR", "data")


# -------------------------------------------------
# Validate required environment variables
# -------------------------------------------------
required_vars = ["BQ_PROJECT_ID", "BQ_RAW_DATASET", "GOOGLE_APPLICATION_CREDENTIALS"]

for var in required_vars:
    if not os.getenv(var):
        raise ValueError(f"Missing required environment variable: {var}")


# -------------------------------------------------
# Initialize BigQuery client
# -------------------------------------------------
client = bigquery.Client.from_service_account_json(
    CREDENTIALS,
    project=PROJECT_ID,
)


# -------------------------------------------------
# CSV → Table mapping
# -------------------------------------------------
FILES = {
    "ai_company_adoption": f"{DATA_DIR}/ai_company_adoption.csv",
    "ai_industry_summary": f"{DATA_DIR}/ai_industry_summary.csv",
    "country_ai_index": f"{DATA_DIR}/country_ai_index.csv",
}


# -------------------------------------------------
# Ensure dataset exists
# -------------------------------------------------
def create_dataset_if_not_exists():

    dataset_id = f"{PROJECT_ID}.{RAW_DATASET}"

    try:
        client.get_dataset(dataset_id)
        logger.info(f"Dataset exists: {dataset_id}")

    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset)
        logger.info(f"Created dataset: {dataset_id}")


# -------------------------------------------------
# CSV → BigQuery loader
# -------------------------------------------------
def load_csv_to_bigquery(table_name: str, file_path: str, chunk_size: int = 10000):

    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"

    logger.info(f"Loading {file_path} → {table_id}")

    if not os.path.exists(file_path):
        logger.warning(f"File not found, skipping: {file_path}")
        return False

    try:

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
        )

        first_chunk = True
        total_rows = 0

        chunks = pd.read_csv(file_path, chunksize=chunk_size)

        for chunk in tqdm(chunks, desc=f"Uploading {table_name}", unit="chunk"):

            if chunk.empty:
                continue

            # overwrite table on first chunk
            if first_chunk:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
                first_chunk = False
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

            job = client.load_table_from_dataframe(
                chunk,
                table_id,
                job_config=job_config,
            )

            job.result()

            total_rows += len(chunk)

        table = client.get_table(table_id)

        logger.info(
            f"Loaded {total_rows:,} rows → {table_id} (table rows: {table.num_rows:,})"
        )

        return True

    except pd.errors.EmptyDataError:

        logger.error(f"CSV file is empty: {file_path}")
        return False

    except GoogleAPIError as e:

        logger.exception(f"BigQuery upload failed: {e}")
        return False

    except Exception as e:

        logger.exception(f"Unexpected error: {e}")
        return False


# -------------------------------------------------
# Main pipeline
# -------------------------------------------------
def main():

    logger.info("=== Starting data ingestion ===")

    create_dataset_if_not_exists()

    success_count = 0

    for table_name, file_path in FILES.items():

        if load_csv_to_bigquery(table_name, file_path):
            success_count += 1

    logger.info(f"=== Ingestion complete === {success_count}/{len(FILES)} tables loaded")


# -------------------------------------------------
# Entry point
# -------------------------------------------------
if __name__ == "__main__":
    main()