import os
import pickle

import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute("SELECT id, language FROM esg_meta WHERE embedded_time IS NULL AND language IS NOT NULL")
    records = cur.fetchall()


files = os.listdir("esg_pickle")

id = [file[:-4] for file in files]

records = [record for record in records if record[0] not in id]

# 将 records 列表分成4份
chunk_size = len(records) // 3
chunks = [records[i * chunk_size:(i + 1) * chunk_size] for i in range(3)]

# 如果 records 的长度不能被3整除，处理剩余的元素
if len(records) % 3 != 0:
    chunks[-1].extend(records[3 * chunk_size:])

# 将每份存成pickle文件
for i, chunk in enumerate(chunks):
    with open(f'chunk_{i}.pkl', 'wb') as f:
        pickle.dump(chunk, f)
