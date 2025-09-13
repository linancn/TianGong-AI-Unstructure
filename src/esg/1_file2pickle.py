import concurrent.futures
import logging
import os
import pickle
import requests
from collections import deque
import time
import random

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="esg_pickle.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

token = os.environ.get("TOKEN")
input_dir = "docs/esg"
output_dir = "docs/processed_docs/esg_pickle"

# Define multiple service endpoints
service_endpoints = [
    "http://localhost:8770/pdf",
    "http://localhost:8771/pdf",
    "http://localhost:8772/pdf",
]

docx_url = "http://localhost:8770/docx"
ppt_url = "http://localhost:8770/ppt"

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id FROM esg_meta WHERE created_time > '2025-04-07' AND unstructure_time IS NULL"
    )
    records = cur.fetchall()


def unstructure_by_service(doc_id, doc_path, token, url):
    """Process document through the appropriate unstructure service"""
    pickle_filename = f"{doc_id}.pkl"
    pickle_path = os.path.join(output_dir, pickle_filename)

    # Check if pickle file already exists
    if os.path.exists(pickle_path):
        logging.info(f"Pickle file already exists for document ID: {doc_id}, skipping.")
        return doc_id

    with open(doc_path, "rb") as f:
        files = {"file": f}
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.post(url, files=files, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            result = response_data.get("result")

            with open(pickle_path, "wb") as pkl_file:
                pickle.dump(result, pkl_file)

            logging.info(
                f"Document ID: {doc_id} processed successfully with service: {url}"
            )
            time.sleep(1 + 2 * random.random())
            return doc_id

        except Exception as e:
            logging.error(
                f"Error processing document ID: {doc_id} with service {url}: {str(e)}"
            )
            return None


def process_documents():
    """Process documents distributing them across available services"""
    documents = []
    for record in records:
        doc_id = record[0]
        doc_path = f"{input_dir}/{doc_id}.pdf"

        if os.path.exists(doc_path):
            documents.append((doc_id, doc_path))
        else:
            logging.info(f"File not found for ID {doc_id}: {doc_path}")

    service_queue = deque(service_endpoints)

    # Process documents using concurrent execution with multiple services
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=len(service_endpoints)
    ) as executor:
        futures = []
        # Distribute documents across services
        for index, (doc_id, doc_path) in enumerate(documents):
            # Get the next service from the queue in round-robin fashion
            service_url = service_queue[0]
            service_queue.rotate(-1)

            # Submit the processing task to the executor
            future = executor.submit(
                unstructure_by_service, doc_id, doc_path, token, service_url
            )
            futures.append((future, doc_id, service_url))

        # Wait for all tasks to complete
        for future, doc_id, service_url in futures:
            try:
                result = future.result()
                if result:
                    logging.info(f"{doc_id} processed with service: {service_url}")
                else:
                    logging.error(
                        f"Failed to process document ID: {doc_id} with service: {service_url}"
                    )
            except Exception as e:
                logging.error(
                    f"Exception while processing document ID: {doc_id}: {str(e)}"
                )


# Run the document processing function
if __name__ == "__main__":
    process_documents()
    conn_pg.close()
