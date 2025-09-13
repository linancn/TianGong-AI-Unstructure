import concurrent.futures
import os
import pickle
import psycopg2

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
dbname = os.getenv("POSTGRES_DB")

with psycopg2.connect(
    f"user={user} password={password} host={host} port={port} dbname={dbname}"
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM standards")
        rows = cur.fetchall()


def process_pdf(record):
    text_list = unstructure_pdf("temp/" + record + ".pdf")

    with open("temp/" + record + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(text_list)

    with open("temp/" + record + ".txt", "w") as f:
        f.write(text_str)


# 从数据库获取的id
db_ids = {row[0] for row in rows}

docs_path = "standards_pickle"
# 列出docs/standards目录下的所有文件
files = os.listdir(docs_path)
file_ids = {os.path.splitext(file_name)[0] for file_name in files}
# 找出数据库中有但文件系统中没有的id
missing_file_ids = db_ids - file_ids


# record = {"id": "rec_clu17n8bslsq4fnfc8s0"}

# record = {"id": "2a7ffbb3-c09d-4010-8edb-b4cc7ff48dbc"}

process_pdf("1088d100-0697-4537-896b-bdb4066bfc1b")

# for record in missing_file_ids:
#     process_pdf(record)

# with concurrent.futures.ProcessPoolExecutor(max_workers=30) as executor:
#     executor.map(process_pdf, missing_file_ids)
