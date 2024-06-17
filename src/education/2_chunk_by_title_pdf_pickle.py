import concurrent.futures
import os
import pickle

import psycopg2
from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf

load_dotenv()

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id, language FROM edu_meta WHERE upload_time IS NOT NULL AND embedding_time IS NULL"
    )
    records = cur.fetchall()


files = os.listdir("education_pickle")

id = [file[:-4] for file in files]

records = [record for record in records if record[0] not in id]

# ids = [record["id"] for record in records]
# print(ids)


def process_pdf(record):
    record_id = record[0]
    if record[1] == "eng":
        language = ["eng"]
    else:
        language = [record[1], "eng"]

    text_list = unstructure_pdf(
        pdf_name="docs/education/" + record_id + ".pdf", languages=language
    )

    text_str_list = [
        "Page {}: {}".format(page_number, text) for text, page_number in text_list
    ]

    with open("education_pickle/" + record_id + ".pdf.pkl", "wb") as f:
        pickle.dump(text_str_list, f)

    text_str = "\n----------\n".join(text_str_list)

    with open("education_txt/" + record_id + ".pdf.txt", "w") as f:
        f.write(text_str)


def safe_process_pdf(record):
    try:
        return process_pdf(record)
    except Exception as e:
        print(f"Error processing {record}: {str(e)}")
        return None


# record = {"id": "af183ae1-c64b-417a-a19d-bf4d9611ce90", "language": "chi_sim"}

# safe_process_pdf(record)

# for record in records:
#     process_pdf(record)


with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
    executor.map(safe_process_pdf, records)
