import concurrent.futures
import os
import pickle

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf
from xata.client import XataClient

load_dotenv()

# xata = XataClient(
#     api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_ESG_DB_URL")
# )

# table_name = "ESG_Reports"
# columns = ["id"]
# filter = {"$notExists": "embeddingTime"}


# def fetch_all_records(xata, table_name, columns, filter, page_size=1000):
#     all_records = []
#     cursor = None
#     more = True

#     while more:
#         page = {"size": page_size}
#         if not cursor:
#             results = xata.data().query(
#                 table_name,
#                 {
#                     "page": page,
#                     "columns": columns,
#                     "filter": filter,
#                 },
#             )
#         else:
#             page["after"] = cursor
#             results = xata.data().query(
#                 table_name,
#                 {
#                     "page": page,
#                     "columns": columns,
#                 },
#             )

#         all_records.extend(results["records"])
#         cursor = results["meta"]["page"]["cursor"]
#         more = results["meta"]["page"]["more"]

#     return all_records


# records = fetch_all_records(xata, table_name, columns, filter)


def process_pdf(record):
    record_id = record["id"]

    text_list = unstructure_pdf("water/" + record_id + ".docx")

    with open("pickle/" + record_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(text_list)

    with open("water/" + record_id + ".txt", "w") as f:
        f.write(text_str)


# record = {"id": "rec_clu17n8bslsq4fnfc8s0"}

record = {"id": "Chap 5_人工智能赋能教学格式"}

process_pdf(record)

# for record in records:
#     process_pdf(record)

# with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
#     executor.map(process_pdf, records)
