import os
import shutil
import re

import psycopg2
from dotenv import load_dotenv

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


# 从数据库获取的id
db_ids = {row[0] for row in rows}

docs_path = "standards_txt"

files = os.listdir(docs_path)

# 遍历目录中的文件名，如果文件名不在数据库id中，则删除文件
for file_name in files:
    file_id = os.path.splitext(file_name)[0]
    if file_id not in db_ids:
        file_path = os.path.join(docs_path, file_name)
        try:
            os.remove(file_path)
            print(f"Deleted {file_path}")
        except OSError as e:
            print(f"Error deleting {file_path}: {e}")

print("Cleanup complete.")
