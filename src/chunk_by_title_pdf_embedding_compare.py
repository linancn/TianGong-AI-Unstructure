import os

from dotenv import load_dotenv
from xata.client import XataClient

load_dotenv()

xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DB_URL"))

result = xata.sql().query(
    'SELECT "id" FROM "ESG_Reports" WHERE "embeddingTime" IS NULL and "report" IS NOT NULL'
)

records = result["records"]
record_ids = set([record["id"] for record in records])

download_files = set(os.path.splitext(file)[0] for file in os.listdir("pickle"))

missing_in_download = record_ids - download_files

print(f"Missing in download: {len(missing_in_download)}")
