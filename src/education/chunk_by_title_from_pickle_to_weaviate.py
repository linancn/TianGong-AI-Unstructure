import glob
import os
import pickle
import re

from bs4 import BeautifulSoup
import tiktoken
import weaviate
import weaviate.classes as wvc
from dotenv import load_dotenv
from weaviate.config import AdditionalConfig

load_dotenv()


def load_pickle_list(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    return data


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    num_tokens = len(encoding.encode(string))
    return num_tokens


def merge_pickle_list(data):
    temp = ""
    result = []
    for d in data:
        if len(d) > 8000:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if table_content:  # If the table is not empty
                    result.append(table_content)
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append(temp + d)
            temp = ""
    if temp:
        result.append(temp)

    return result


def fix_utf8(original_list):
    cleaned_list = []
    for original_str in original_list:
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append(cleaned_str)
    return cleaned_list


def split_chunks(text_list: list, source: str):
    chunks = []
    for text in text_list:
        chunks.append({"content": text, "source": source})
    return chunks


directory = "pickle"

w_client = weaviate.connect_to_local(
    host="localhost", additional_config=AdditionalConfig(timeout=(600, 800))
)

for file in os.listdir(directory):

    file_path = os.path.join(directory, file)
    file_name_without_ext = re.split(r"\.pkl$", file)[0]

    data = load_pickle_list(file_path)
    data = merge_pickle_list(data)
    data = fix_utf8(data)

    chunks = split_chunks(text_list=data, source=file_name_without_ext)

    zhongzi = w_client.collections.get(name="zhongzi")
    zhongzi.data.insert_many(chunks)

    print("Done!")
    w_client.close()
