import mimetypes
import os

from dotenv import load_dotenv
from xata.client import XataClient

load_dotenv()


def get_extension_from_content_type(content_type):
    extension = mimetypes.guess_extension(content_type)
    return extension


xata = XataClient(api_key=os.getenv("XATA_API_KEY"), db_url=os.getenv("XATA_ESG_DB_URL"))

data = xata.data().query(
    "ESG_Reports",
    {
        "columns": ["id"],
        "page": {"size": 1000},
    },
)


for record in data["records"]:
    file = xata.files().get("ESG_Reports", record["id"], "report")
    ext = get_extension_from_content_type(file.headers["Content-Type"])
    with open("download/" + record["id"] + ext, "wb") as f:
        f.write(file.content)
    print(f"Downloaded {record['id']}{ext}")
