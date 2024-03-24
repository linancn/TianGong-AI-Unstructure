import logging
import os
import pickle
from datetime import UTC, datetime

import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from tenacity import retry, stop_after_attempt, wait_fixed
from xata import XataClient

load_dotenv()

logging.basicConfig(
    filename="esg_embedding.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

client = OpenAI()

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_ESG_DB_URL")

xata = XataClient(
    api_key=xata_api_key,
    db_url=xata_db_url,
)

pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY"))
idx = pc.Index(os.environ.get("PINECONE_SERVERLESS_INDEX_NAME"))


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    num_tokens = len(encoding.encode(string))
    return num_tokens


def fix_utf8(original_list):
    cleaned_list = []
    for original_str in original_list:
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append(cleaned_str)
    return cleaned_list


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_embeddings(text_list, model="text-embedding-3-small"):
    try:
        text_list = [text.replace("\n\n", " ").replace("\n", " ") for text in text_list]
        length = len(text_list)
        results = []
        for i in range(0, length, 1000):
            results.append(
                client.embeddings.create(
                    input=text_list[i : i + 1000], model=model
                ).data
            )
        return sum(results, [])

    except Exception as e:
        print(e)


def load_pickle_list(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    return data


def merge_pickle_list(data):
    temp = ""
    result = []
    for d in data:
        if len(d) > 8000:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if table_content:  # 确保表格内容不为空
                    result.append(table_content)
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append(temp + d)
            temp = ""
    if temp:
        result.append(temp)

    return result


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="esg", show_progress=False
        )
    except Exception as e:
        logging.error(e)


dir = "esg_pickle"

aa = os.listdir(dir)

for file in os.listdir(dir):

    try:
        file_path = os.path.join(dir, file)
        data = load_pickle_list(file_path)
        data = merge_pickle_list(data)
        data = fix_utf8(data)
        embeddings = get_embeddings(data)

        file_id = file.split(".")[0]

        vectors = []
        fulltext_list = []
        for index, e in enumerate(embeddings):
            fulltext_list.append(
                {
                    "sortNumber": index,
                    "text": data[index],
                    "reportId": file_id,
                }
            )
            vectors.append(
                {
                    "id": file_id + "_" + str(index),
                    "values": e.embedding,
                    "metadata": {
                        "text": data[index],
                        "rec_id": file_id,
                    },
                }
            )

        n = len(fulltext_list)
        for i in range(0, n, 500):
            batch = fulltext_list[i : i + 500]
            result = xata.records().bulk_insert("ESG_Fulltext", {"records": batch})

        logging.info(
            f"{file_id} fulltext insert finished, with status_code: {result.status_code}"
        )

        upsert_vectors(vectors)

        embedded = xata.records().update(
            "ESG_Reports", file_id, {"embeddingTime": datetime.now(UTC).isoformat()}
        )

        logging.info(f"{file_id} embedding finished")

    except Exception as e:
        logging.error(e)
        continue
