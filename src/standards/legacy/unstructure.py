import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO

import pandas as pd
from psycopg2 import pool
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from opensearchpy import OpenSearch

# Load environment variables
load_dotenv()

# Extract IDs from pickle files
pickle_dir = "docs/processed_docs/standards_pickle"
file_ids = []
for file in os.listdir(pickle_dir):
    if file.endswith('.pkl'):
        file_id = file.split('.')[0]  # Extract id from filename
        file_ids.append(file_id)

logging.info(f"Found {len(file_ids)} IDs from pickle files")



conn_pool = pool.SimpleConnectionPool(
    1, 20,  # min and max number of connections
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


# Get IDs from the database
db_ids = []
with conn_pool.getconn() as conn_pg:
    with conn_pg.cursor() as cur:
        cur.execute("SELECT id FROM standards")
        records = cur.fetchall()
        db_ids = [str(record[0]) for record in records]
    conn_pool.putconn(conn_pg)

logging.info(f"Found {len(db_ids)} IDs from database")

# Find overlapping IDs
overlapping_ids = list(set(file_ids).intersection(set(db_ids)))
logging.info(f"Found {len(overlapping_ids)} overlapping IDs")

# Update unstructure_time for overlapping IDs
batch_size = 10
for i in range(0, len(overlapping_ids), batch_size):
    batch_ids = overlapping_ids[i:i+batch_size]
    conn_pg = conn_pool.getconn()
    try:
        with conn_pg.cursor() as cur:
            # Create batch of update parameters
            args = [(datetime.now(UTC), file_id) for file_id in batch_ids]
            cur.executemany(
                "UPDATE standards SET unstructure_time = %s WHERE id = %s",
                args
            )
            conn_pg.commit()
            logging.info(f"Updated unstructure_time for batch {i//batch_size + 1} ({len(batch_ids)} records)")
    except Exception as e:
        logging.error(f"Error updating batch {i//batch_size + 1}: {e}")
    finally:
        # Release the connection back to the pool
        conn_pool.putconn(conn_pg)


conn_pool.closeall()