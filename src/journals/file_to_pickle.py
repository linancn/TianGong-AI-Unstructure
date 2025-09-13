import os
import requests
import pickle
from urllib.parse import quote
from datetime import UTC, datetime
import psycopg2
import psycopg2.pool
import logging
from dotenv import load_dotenv
import concurrent.futures

load_dotenv()

# --- Constants ---
LOG_FILENAME = "journal_redo_1.log"
BASE_DIR = "docs/journals/"
OUTPUT_DIR = "docs/processed_docs/journal_new_pickle"
PDF_URL = "http://localhost:8770/mineru_sci"
MAX_WORKERS = 4
REQUEST_TIMEOUT = 1200  # 20 minutes

# --- Logging Setup ---
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)

# --- Environment & Globals ---
token = os.environ.get("TOKEN")
db_pool = None


# --- Functions ---
def init_db_pool():
    """Initializes the database connection pool."""
    global db_pool
    try:
        db_pool = psycopg2.pool.ThreadSafeConnectionPool(
            minconn=1,
            maxconn=MAX_WORKERS,  # Max connections equal to worker threads
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        logging.info("Database connection pool created successfully.")
    except psycopg2.OperationalError as e:
        logging.error(f"Failed to create database connection pool: {e}")
        raise


def close_db_pool():
    """Closes all connections in the pool."""
    if db_pool:
        db_pool.closeall()
        logging.info("Database connection pool closed.")


def unstructure_by_service(doc_path, file_id, url, token):
    """Sends a document to the unstructuring service and saves the result."""
    pickle_filename = f"{file_id}.pkl"
    pickle_path = os.path.join(OUTPUT_DIR, pickle_filename)

    with open(doc_path, "rb") as f:
        files = {"file": f}
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.post(
                url, files=files, headers=headers, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            response_data = response.json()
            result = response_data.get("result")

            with open(pickle_path, "wb") as pkl_file:
                pickle.dump(result, pkl_file)

            return True
        except requests.exceptions.Timeout:
            logging.error(f"Request timed out for file_id: {file_id} at URL: {url}")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for file_id: {file_id}: {e}")
            return False


def process_journal_entry(result):
    """Processes a single journal entry."""
    file_id = result[0]
    doi = result[1]

    if file_id == "5d2c326d-fec8-495d-84b9-0a81d1b9ecf0":
        logging.info(f"Skipping problematic file_id: {file_id}")
        return

    pickle_path = os.path.join(OUTPUT_DIR, f"{file_id}.pkl")
    if os.path.exists(pickle_path):
        return

    coded_doi = quote(quote(doi))
    file_path = os.path.join(BASE_DIR, coded_doi + ".pdf")

    if not os.path.exists(file_path):
        logging.info(f"PDF file not found for {file_id}: {file_path}")
        return

    logging.info(f"Processing {file_id} at {file_path}")
    if not unstructure_by_service(file_path, file_id, PDF_URL, token):
        return  # Stop processing this entry if unstructuring fails

    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE journals SET upload_time = %s WHERE id = %s",
                (datetime.now(UTC), file_id),
            )
            conn.commit()
        logging.info(f"Updated upload_time for {file_id}")
    except Exception as e:
        logging.error(f"DB Error processing {file_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)


def main():
    """Main function to run the script."""
    init_db_pool()

    try:
        with open("journals.pkl", "rb") as f:
            data = pickle.load(f)

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_journal_entry, data)

        logging.info("All files processed.")
    finally:
        close_db_pool()


if __name__ == "__main__":
    main()
