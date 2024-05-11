import concurrent.futures
import os
import pickle

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf
from xata.client import XataClient

load_dotenv()

xata = XataClient(
    api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DOCS_DB_URL")
)

table_name = "reports"
columns = ["id", "language"]
filter = {"$notExists": "embedding_time"}


def fetch_all_records(xata, table_name, columns, filter, page_size=1000):
    all_records = []
    cursor = None
    more = True

    while more:
        page = {"size": page_size}
        if not cursor:
            results = xata.data().query(
                table_name,
                {
                    "page": page,
                    "columns": columns,
                    "filter": filter,
                },
            )
        else:
            page["after"] = cursor
            results = xata.data().query(
                table_name,
                {
                    "page": page,
                    "columns": columns,
                },
            )

        all_records.extend(results["records"])
        cursor = results["meta"]["page"]["cursor"]
        more = results["meta"]["page"]["more"]

    return all_records


records = fetch_all_records(xata, table_name, columns, filter)


def process_pdf(record):
    record_id = record["id"]
    if record["language"] == "eng":
        language = ["eng"]
    else:
        language = [record["language"], "eng"]

    text_list = unstructure_pdf(
        pdf_name="docs/reports/" + record_id + ".pdf", languages=language
    )

    with open("reports_pickle/" + record_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(text_list)

    with open("reports_txt/" + record_id + ".txt", "w") as f:
        f.write(text_str)

    # text_list = unstructure_pdf(
    #     pdf_name="pickle_single/pdf/" + record_id + ".pdf", languages=language
    # )

    # with open("pickle_single/pickle/" + record_id + ".pkl", "wb") as f:
    #     pickle.dump(text_list, f)

    # text_str = "\n----------\n".join(text_list)

    # with open("pickle_single/txt/" + record_id + ".txt", "w") as f:
    #     f.write(text_str)


# record = {"id": "rec_cospqe45251e3okho9g0", "language": "eng"}

# process_pdf(record)

# for record in records:
#     process_pdf(record)

with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
    executor.map(process_pdf, records)
