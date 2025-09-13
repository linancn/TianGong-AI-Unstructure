import os
import requests
import pickle
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="供应商1.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

token = os.environ.get("TOKEN")

output_dir = "test"


def unstructure_by_service(doc_path, url, token):
    with open(doc_path, "rb") as f:
        base_name = os.path.basename(doc_path)
        name_without_ext = os.path.splitext(base_name)[0]
        pickle_path = os.path.join(output_dir, f"{name_without_ext}.pkl")

        files = {"file": f}
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, files=files, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        result = response_data.get("result")

        with open(pickle_path, "wb") as pkl_file:
            pickle.dump(result, pkl_file)


dir_path = "test"

# pdf_url = 'http://localhost:8770/pdf'
pdf_url = "http://localhost:8770/mineru"
# pdf_url = 'http://192.168.8.1:7770/mineru'
docx_url = "http://localhost:8770/docx"
ppt_url = "http://localhost:8770/ppt"


for doc in os.listdir(dir_path):
    if doc.endswith(".pdf"):
        doc_path = os.path.join(dir_path, doc)
        base_name = os.path.basename(doc_path)
        name_without_ext = os.path.splitext(base_name)[0]
        pickle_path = os.path.join(output_dir, f"{name_without_ext}.pkl")
        if not os.path.exists(pickle_path):
            try:
                unstructure_by_service(doc_path, pdf_url, token)
                logging.info(f"Processed successfully: {doc}")
            except requests.RequestException as e:
                logging.error(f"Failed to process {doc}: {e}")
                continue
    elif doc.endswith(".docx"):
        doc_path = os.path.join(dir_path, doc)
        unstructure_by_service(doc_path, docx_url, token)
    elif doc.endswith(".pptx"):
        doc_path = os.path.join(dir_path, doc)
        unstructure_by_service(doc_path, ppt_url, token)
