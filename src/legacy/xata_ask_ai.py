import base64
import os

import pandas as pd
from dotenv import load_dotenv
from xata.client import XataClient

load_dotenv()

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_DB_URL")
xata = XataClient(api_key=xata_api_key, db_url=xata_db_url)

result = xata.data().ask(
    "ESG_Embeddings",  # reference table
    "越南工厂的女性员工比例有多高？",  # question to ask
    options={
        "searchType": "vector",
        "vectorSearch": {
            "column": "vector",
            "contentColumn": "text",
            "filter": {
                # ...search filter options...
            },
        },
    },
)

print(result)
