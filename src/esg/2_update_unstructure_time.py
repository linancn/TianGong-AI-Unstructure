import logging
import os
import time

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="update_unstructure_time.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

output_dir = "docs/processed_docs/esg_pickle"

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id FROM esg_meta WHERE created_time > '2025-04-07' AND unstructure_time IS NULL"
    )
    records = cur.fetchall()

def update_unstructure_time():
    logging.info(f"Found {len(records)} records to process")
    update_list = []
    
    # First collect all records to update
    for record in records:
        id = record[0]
        pickle_path = os.path.join(output_dir, f"{id}.pkl")
        
        if os.path.exists(pickle_path):
            # Get file creation time
            creation_time = os.path.getctime(pickle_path)
            creation_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(creation_time))
            update_list.append((creation_time_str, id))
    
    # Then update the database with a single commit
    if update_list:
        try:
            with conn_pg.cursor() as cur:
                cur.executemany(
                    "UPDATE esg_meta SET unstructure_time = %s WHERE id = %s",
                    update_list
                )
            conn_pg.commit()
            logging.info(f"Successfully updated unstructure_time for {len(update_list)} documents")
        except Exception as e:
            logging.error(f"Failed to update unstructure_time for batch: {str(e)}")
            conn_pg.rollback()

# Execute the function directly
try:
    update_unstructure_time()
except Exception as e:
    logging.error(f"Error in update_unstructure_time: {str(e)}")
finally:
    conn_pg.close()
    logging.info("Database connection closed")


