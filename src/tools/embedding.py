import os
import pickle

import tiktoken
from dotenv import load_dotenv
from openai import OpenAI
from xata import XataClient
from xata.helpers import BulkProcessor

load_dotenv()

client = OpenAI()

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_DB_URL")

xata = XataClient(
    api_key=xata_api_key,
    db_url=xata_db_url,
)
bp = BulkProcessor(client=xata, batch_size=500)


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    num_tokens = len(encoding.encode(string))
    return num_tokens


def get_embeddings(text_list, model="text-embedding-ada-002"):
    text_list = [text.replace("\n\n", " ").replace("\n", " ") for text in text_list]
    return client.embeddings.create(input=text_list, model=model).data


def load_pickle_list(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    return data


def merge_pickle_list(data):
    temp = ""
    result = []
    for d in data:
        if num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append(temp + d)
            temp = ""
    if temp:
        result.append(temp)

    return result


dir = "pickle"

for file in os.listdir(dir):
    datalist = []
    file_path = os.path.join(dir, file)
    data = load_pickle_list(file_path)
    data = merge_pickle_list(data)
    embeddings = get_embeddings(data)

    file_id = file.split(".")[0]

    for index, e in enumerate(embeddings):
        datalist.append(
            {
                "reportId": file_id,
                "sortNumber": index,
                "vector": e.embedding,
                "text": data[index],
            }
        )

    bp.put_records("ESG_Embeddings", datalist)
    print(f"{file_id} embedding finished.")
