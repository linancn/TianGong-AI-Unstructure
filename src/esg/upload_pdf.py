import os

from dotenv import load_dotenv
from xata.client import XataClient

load_dotenv()

xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_DB_URL"))

for file in os.listdir("download"):
    if file.endswith(".pdf"):
        record_id = file.split(".")[0]
        # if record_id == "rec_cm1s4g6q2mhohok8sif0":
        file_content = open("download/" + file, "rb")
        response = xata.files().put(
            table_name="ESG_Reports",
            record_id=record_id,
            column_name="report",
            data=file_content,
            content_type="application/pdf",
        )
        print(record_id + ": " + str(response.content))
