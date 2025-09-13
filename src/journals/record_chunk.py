import os
import pickle
from urllib.parse import quote

import psycopg2

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


# with conn_pg.cursor() as cur:
#     cur.execute(
#         "SELECT doi,journal,date FROM journals WHERE upload_time IS NOT NULL"
#     )
#     results = cur.fetchall()

with conn_pg.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM journals WHERE upload_time IS NOT NULL")
    total_records = cur.fetchone()[0]
    offset = 0
    batch_size = 3000

    results = []
    while offset < total_records:
        cur.execute(
            "SELECT doi, journal, date FROM journals WHERE upload_time IS NOT NULL LIMIT %s OFFSET %s",
            (batch_size, offset),
        )
        batch_results = cur.fetchall()
        if not batch_results:
            break
        results.extend(batch_results)
        offset += batch_size

pdf_list = []
for record in results:
    pdf_path = quote(quote(record[0] + ".pdf"))
    pdf_list.append(
        {
            "doi": record[0],
            "pdf_path": pdf_path,
            "journal": record[1],
            "date": record[2],
        }
    )


def get_file_paths(directory):
    file_paths = set()
    for root, _, files in os.walk(directory):
        for file in files:
            relative_path = os.path.relpath(os.path.join(root, file), directory)
            file_paths.add(relative_path)
    return file_paths


def get_base_paths(file_paths, extension_length=4):
    return {path[:-extension_length] for path in file_paths}


pdf_directory = "docs/journals/"
pickle_directory = "processed_docs/journal_pickle/"
existing_pdf_paths = get_file_paths(pdf_directory)
existing_pickle_paths = get_base_paths(get_file_paths(pickle_directory))
missing_pdf_paths = existing_pdf_paths - existing_pickle_paths

missing_pdf_list = [pdf for pdf in pdf_list if pdf["pdf_path"] in missing_pdf_paths]

for pdf in missing_pdf_list:
    pdf["pdf_path"] = "docs/journals/" + pdf["pdf_path"]


# 将 missing_pdf_list 平均分成4份
def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


# 分割后的列表
split_pdf_lists = split_list(missing_pdf_list, 3)

# 保存每份数据到一个pickle文件中
for i, pdf_list_part in enumerate(split_pdf_lists):
    with open(f"journal_pdf_list_{i}.pkl", "wb") as f:
        pickle.dump(pdf_list_part, f)

conn_pg.close()


# missing_pdf_paths_aa = set([pdf['pdf_path'] for pdf in missing_pdf_list])
# pdf_list_a = missing_pdf_paths - missing_pdf_paths_aa

# for pickle_path in pdf_list_a:
#     original_path = os.path.join(pdf_directory, pickle_path)
#     unencoded_path = unquote(unquote(original_path))

#     if os.path.exists(original_path):
#         os.rename(original_path, unencoded_path)
#         print(f"Renamed {original_path} to {unencoded_path}")
