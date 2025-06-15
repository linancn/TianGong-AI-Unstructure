import concurrent.futures
import os
import pickle
from datetime import datetime
import psycopg2
from psycopg2 import sql

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf

load_dotenv()

conn = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

cur = conn.cursor()
cur.execute("SELECT id, language FROM ali WHERE file_type = '.pdf'")
records = cur.fetchall()


files = os.listdir("processed_docs/ali_pickle")

id = [file[:-4] for file in files]

records = [record for record in records if record[0] not in id]


def process_pdf(record):
    record_id = record[0]
    language = [record[1]]

    text_list = unstructure_pdf(
        pdf_name="docs/ali/" + record_id + ".pdf", languages=language
    )

    with open("processed_docs/ali_pickle/" + record_id + ".pdf" + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str_list = [
        "Page {}: {}".format(page_number, text) for text, page_number in text_list
    ]

    text_str = "\n----------\n".join(text_str_list)

    with open("processed_docs/ali_txt/" + record_id + ".pdf" + ".txt", "w") as f:
        f.write(text_str)


def safe_process_pdf(record):
    try:
        return process_pdf(record)
    except Exception as e:
        print(f"Error processing {record}: {str(e)}")
        return None


# record = {"id": "af183ae1-c64b-417a-a19d-bf4d9611ce90", "language": "chi_sim"}

# safe_process_pdf(record)

for record in records:
    process_pdf(record)
    cur.execute(
        sql.SQL("UPDATE ali SET unstructure_time = %s WHERE id = %s"),
        [datetime.now(), record[0]],
    )
    conn.commit()

cur.close()
conn.close()
print("Data unstructured successfully")

# with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
#     executor.map(safe_process_pdf, records)
