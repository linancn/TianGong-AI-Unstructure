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
from weaviate.classes.config import Configure, DataType, Property
# from tiktoken import TokenCounts

load_dotenv()


def load_pickle_list(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    return data

# def num_tokens_from_string(string: str) -> int:
#     """Returns the number of tokens in a text string."""
#     token_counts = TokenCounts()
#     if not isinstance(string, str):
#         string = str(string)
#     num_tokens = len(token_counts.count_tokens(string))
#     return num_tokens

def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    num_tokens = len(encoding.encode(string))
    return num_tokens


def merge_pickle_list(data):
    temp = ""
    result = []
    for d in data:
        if not isinstance(d, str):  # If d is not a string
            d = str(d)  # Convert d to a string
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
        if "\n\n" in text:
            title, content = text.split("\n\n", 1)
        else:
            title = "default"  # 设置默认标题
            content = text
        if not content:  # 如果内容为空
            content = "default"  # 设置默认内容
        chunks.append({"title": title, "content": content, "source": source})
    return chunks


directory = "pickle"

w_client = weaviate.connect_to_local(
    host="localhost", port=8088, additional_config=AdditionalConfig(timeout=(600, 800))
)

try:
    collection = w_client.collections.create(
        name="Education",
        properties=[
            Property(name="title", data_type=DataType.TEXT),
            Property(name="content", data_type=DataType.TEXT),
            Property(name="source", data_type=DataType.TEXT),
        ],
        vectorizer_config=[
            Configure.NamedVectors.text2vec_transformers(
                name="title", source_properties=["title"]
            ),
            Configure.NamedVectors.text2vec_transformers(
                name="content", source_properties=["content"]
            ),
        ],
    )

    for file in os.listdir(directory):

        file_path = os.path.join(directory, file)
        file_name_without_ext = re.split(r"\.pkl$", file)[0]

        data = load_pickle_list(file_path)
        data = merge_pickle_list(data)
        data = fix_utf8(data)

        chunks = split_chunks(text_list=data, source=file_name_without_ext)

        collection.data.insert_many(chunks)

        print("Done!")
finally:
    w_client.close()
