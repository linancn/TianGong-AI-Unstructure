import concurrent.futures
import os
from urllib.parse import quote

import psycopg2

from tools.chunk_by_sci_pdf import sci_chunk


conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT doi,journal,date FROM journals WHERE upload_time IS NOT NULL AND embedding_time IS NULL"
    )
    results = cur.fetchall()

pdf_list = []
for record in results:
    pdf_path = quote(quote("docs/journals/" + record[0] + ".pdf"))
    pdf_list.append(
        {
            "doi": record[0],
            "pdf_path": pdf_path,
            "journal": record[1],
            "date": record[2],
        }
    )

# for pdf in pdf_list:
#     sci_chunk(pdf)

# pdf_files = get_contained_list(all_records, pdf_names)
# # pdf_names = pdf_names[0:100]
# jie_pdf_names = []
# for pdf_name in pdf_names:
#     if "jiec" in pdf_name:
#         jie_pdf_names.append(pdf_name)

# test = {
#     "doi": "10.1002/aenm.202400742",
#     "pdf_path": "docs/journals/10.1002/aenm.202400742.pdf",
#     "journal": "test",
#     "date": "2020-02",
# }

# sci_chunk(test)


def safe_sci_chunk(pdf):
    try:
        return sci_chunk(pdf)
    except Exception as e:
        print(f"Error processing {pdf}: {str(e)}")
        return None


# start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(30) as executor:
    executor.map(safe_sci_chunk, pdf_list)

# end_time = time.time()


# print(f"Execution time: {end_time - start_time} seconds")
