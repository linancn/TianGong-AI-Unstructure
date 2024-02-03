import concurrent.futures
import os
import time

from xata.client import XataClient

from tools.chunk_by_sci_pdf import sci_chunk

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_DOCS_DB_URL")
xata = XataClient(api_key=xata_api_key, db_url=xata_db_url)


def get_contained_list(list1, list2):
    return [s2 for s2 in list2 if any(s1["doi"] in s2 for s1 in list1)]


table_name = "journals"
columns = ["doi", "journal", "date"]
filter = {
    "$all": [
        {"$exists": "upload_time"},
        {"$notExists": "embedding_time"},
        # {"journal": "JOURNAL OF INDUSTRIAL ECOLOGY"},
    ]
}


# def get_all_records(
#     xata, table_name, columns, filter, page_size=1000, offset=0, all_records=[]
# ):
#     page = {"size": page_size, "offset": offset}

#     while True:
#         data = xata.data().query(
#             table_name,
#             {
#                 "page": page,
#                 "columns": columns,
#                 "filter": filter,
#             },
#         )

#         if not data["records"]:
#             return all_records

#         all_records.extend(data["records"])

#         return get_all_records(
#             xata=xata,
#             table_name=table_name,
#             columns=columns,
#             filter=filter,
#             offset=offset + page_size,
#             all_records=all_records,
#         )


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


all_records = fetch_all_records(xata, table_name, columns, filter)

pdf_list = []
for record in all_records:
    pdf_list.append(
        {
            "pdf_path": "docs/journals/" + record["doi"] + ".pdf",
            "journal": record["journal"],
            "date": record["date"],
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

# test={"pdf_path": "docs/journals/10.1046/j.1365-294x.1997.00205.x.pdf",
#             "journal": "test",
#             "date": "2020-02",}

# sci_chunk(test)


def safe_sci_chunk(pdf):
    try:
        return sci_chunk(pdf)
    except Exception as e:
        print(f"Error processing {pdf}: {str(e)}")
        return None

start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(16) as executor:
    executor.map(safe_sci_chunk, pdf_list)

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")
