import os
import shutil

import psycopg2
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
dbname = os.getenv("POSTGRES_DB")

conn = psycopg2.connect(
    f"user={user} password={password} host={host} port={port} dbname={dbname}"
)

cur = conn.cursor()

cur.execute("SELECT id, xata_id FROM reports")

rows = cur.fetchall()


for row in rows:
    old_filename = f"reports_txt/{row[1]}.txt"
    new_filename = f"reports_txt/{row[0]}.txt"
    if os.path.exists(old_filename):
        shutil.move(old_filename, new_filename)

cur.close()
conn.close()
