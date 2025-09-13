import os
import pickle

import psycopg2

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id, doi FROM journals WHERE upload_time < '2025-06-25T00:00:00+00:00' ORDER BY TO_DATE(date || '-01', 'YYYY-MM-DD') DESC"
    )
    results = cur.fetchall()

# 将结果分成4份，交替分配
part1 = []
part2 = []
part3 = []
part4 = []

for i, result in enumerate(results):
    if i % 4 == 0:
        part1.append(result)
    elif i % 4 == 1:
        part2.append(result)
    elif i % 4 == 2:
        part3.append(result)
    else:
        part4.append(result)

# 保存4个部分到pickle文件
with open("part1_journals.pkl", "wb") as f:
    pickle.dump(part1, f)

with open("part2_journals.pkl", "wb") as f:
    pickle.dump(part2, f)

with open("part3_journals.pkl", "wb") as f:
    pickle.dump(part3, f)

with open("part4_journals.pkl", "wb") as f:
    pickle.dump(part4, f)
