import concurrent.futures
import os
import pickle

from dotenv import load_dotenv
from xata.client import XataClient

from tools.unstructure_pdf import unstructure_pdf

load_dotenv()

xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DB_URL"))

result = xata.sql().query(
    'SELECT "id" FROM "ESG_Reports" WHERE "embeddingTime" IS NULL and "report" IS NOT NULL'
)

records = result["records"]

# for record in records:
#     record_id = record["id"]
#     report_file = xata.files().get("ESG_Reports", record_id, "report")
#     with open("download/" + record_id + ".pdf", "wb") as f:
#         f.write(report_file.content)


def process_pdf(record):
    record_id = record["id"]

    text_list = unstructure_pdf("download/" + record_id + ".pdf")

    with open("pickle/" + record_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(text_list)

    with open("txt/" + record_id + ".txt", "w") as f:
        f.write(text_str)

# record = {"id": "rec_clu17n8bslsq4fnfc8s0"}

# process_pdf(record)

# for record in records:
#     process_pdf(record)

with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
    executor.map(process_pdf, records)

