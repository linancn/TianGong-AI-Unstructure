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
columns = ["doi"]
filter = {
    "$all": [
        {"$exists": "upload_time"},
        {"$notExists": "embedding_time"},
        {"journal": "JOURNAL OF INDUSTRIAL ECOLOGY"},
    ]
}


def get_all_records(
    xata, table_name, columns, filter, page_size=1000, offset=0, all_records=[]
):
    page = {"size": page_size, "offset": offset}

    while True:
        data = xata.data().query(
            table_name,
            {
                "page": page,
                "columns": columns,
                "filter": filter,
            },
        )

        if not data["records"]:
            return all_records

        all_records.extend(data["records"])

        return get_all_records(
            xata=xata,
            table_name=table_name,
            columns=columns,
            filter=filter,
            offset=offset + page_size,
            all_records=all_records,
        )


all_records = get_all_records(xata, table_name, columns, filter)

# directory = "docs/journals"

# pdf_names = []
# for dirpath, dirnames, filenames in os.walk(directory):
#     for filename in filenames:
#         pdf_names.append(os.path.join(dirpath, filename))


# pdf_files = get_contained_list(all_records, pdf_names)
# # pdf_names = pdf_names[0:100]
# jie_pdf_names = []
# for pdf_name in pdf_names:
#     if "jiec" in pdf_name:
#         jie_pdf_names.append(pdf_name)

# sci_chunk("docs/journals/10.1007/s11356-022-21798-3.pdf")

# pdf_names = pdf_names[0:100]

start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(16) as executor:
    executor.map(sci_chunk, jie_pdf_names)

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")
