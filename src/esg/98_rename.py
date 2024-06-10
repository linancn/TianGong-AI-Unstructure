import glob
import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO

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
    cur.execute("SELECT id, xata_id FROM esg_meta WHERE xata_id IS NOT NULL")
    records = cur.fetchall()

dir = "esg_txt/*"
files = glob.glob(dir)

for file in files:
    # Get xata_id from the file name
    xata_id = os.path.splitext(os.path.basename(file))[0]

    # Find the id corresponding to the xata_id in records
    id = next((record[0] for record in records if record[1] == xata_id), None)

    if id is not None:
        # Rename the file with the found id
        os.rename(
            file,
            os.path.join(os.path.dirname(file), str(id) + os.path.splitext(file)[1]),
        )
