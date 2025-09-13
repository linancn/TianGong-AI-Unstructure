import concurrent.futures
import os
import pickle
import requests

import psycopg2
from dotenv import load_dotenv

load_dotenv()

token = os.environ.get("TOKEN")
input_dir = "docs/education"
output_dir = "docs/processed_docs/education_pickle"

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id, file_type FROM edu_meta WHERE upload_time IS NOT NULL AND embedding_time IS NULL"
    )
    records = cur.fetchall()


def unstructure_by_service(doc_id, doc_path, file_type, token):
    """Process document through the appropriate unstructure service based on file type"""
    with open(doc_path, "rb") as f:
        # Set pickle filename to id.filetype.pickle
        pickle_filename = f"{doc_id}.{file_type}.pickle"
        pickle_path = os.path.join(output_dir, pickle_filename)

        # Select the appropriate URL based on file type
        if file_type.lower() == "pdf":
            url = pdf_url
        elif file_type.lower() in ["docx", "doc"]:
            url = docx_url
        elif file_type.lower() in ["pptx", "ppt"]:
            url = ppt_url
        else:
            print(f"Unsupported file type: {file_type} for document ID: {doc_id}")
            return None

        files = {"file": f}
        headers = {"Authorization": f"Bearer {token}"}

        try:
            response = requests.post(url, files=files, headers=headers)
            response.raise_for_status()
            response_data = response.json()
            result = response_data.get("result")

            with open(pickle_path, "wb") as pkl_file:
                pickle.dump(result, pkl_file)

        except Exception as e:
            print(f"Error processing document ID: {doc_id}: {str(e)}")
            return None


pdf_url = "http://localhost:8770/pdf"
docx_url = "http://localhost:8770/docx"
ppt_url = "http://localhost:8770/ppt"


# Process documents using concurrent execution
def process_documents():
    # Get file paths from database for each ID
    with conn_pg.cursor() as cur:
        for record in records:
            doc_id = record[0]  # First element is the id
            file_type = record[1]  # Second element is the file_type
            doc_path = f"{input_dir}/{doc_id}.{file_type}"

            if os.path.exists(doc_path):
                unstructure_by_service(doc_id, doc_path, file_type, token)
            else:
                print(f"File not found for ID {doc_id}: {doc_path}")


# Run the document processing function
if __name__ == "__main__":
    process_documents()
    conn_pg.close()
