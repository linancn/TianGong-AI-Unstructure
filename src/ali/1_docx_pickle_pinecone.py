import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO

import pandas as pd
import psycopg2
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

logging.basicConfig(
    filename="ali_docx_embedding.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)

client = OpenAI()
pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY_US_EAST_1"))
idx = pc.Index(os.environ.get("PINECONE_SERVERLESS_INDEX_NAME_US_EAST_1"))


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(string))


def fix_utf8(original_list):
    cleaned_list = []
    for original_str in original_list:
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append(cleaned_str)
    return cleaned_list


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_embeddings(items, model="text-embedding-3-small"):
    text_list = [item for item in items]
    try:
        text_list = [text.replace("\n\n", " ").replace("\n", " ") for text in text_list]
        length = len(text_list)
        results = []
        for i in range(0, length, 1000):
            result = client.embeddings.create(
                input=text_list[i : i + 1000], model=model
            ).data
            results += result
        return results

    except Exception as e:
        logging.error(f"Error generating embeddings: {e}")
        raise


def load_pickle_list(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    return data


def split_dataframe_table(html_table, chunk_size=8100):
    dfs = pd.read_html(StringIO(html_table))
    if not dfs:
        return []

    df = dfs[0]
    tables = []
    sub_df = pd.DataFrame()
    token_count = 0

    for _, row in df.iterrows():
        row_html = row.to_frame().T.to_html(index=False, border=0, classes=None)
        row_token_count = num_tokens_from_string(row_html)

        if token_count + row_token_count > chunk_size and not sub_df.empty:
            sub_html = sub_df.to_html(index=False, border=0, classes=None)
            tables.append(sub_html)
            sub_df = pd.DataFrame()
            token_count = 0

        sub_df = pd.concat([sub_df, row.to_frame().T])
        token_count += row_token_count

    if not sub_df.empty:
        sub_html = sub_df.to_html(index=False, border=0, classes=None)
        tables.append(sub_html)

    return tables


def merge_pickle_list(data):
    temp = ""
    result = []
    for d in data:
        if num_tokens_from_string(d) > 8100:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    if table_content:  # 确保表格内容不为空
                        result.append(table_content)
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append(str(soup))
                    except Exception as e:
                        logging.error(f"Error splitting dataframe table: {e}")
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append((temp + d))
            temp = ""
    if temp:
        result.append(temp)

    return result


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="ali", show_progress=False
        )
    except Exception as e:
        logging.error(f"Error upserting vectors: {e}")
        raise

conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id, title FROM ali WHERE file_type = '.docx'"
    )
    records = cur.fetchall()

ids = [record[0] for record in records]
titles = {record[0]: record[1] for record in records}

files = [str(id) + ".docx.pkl" for id in ids]

dir = "processed_docs/ali_pickle"


for file in files:
    file_path = os.path.join(dir, file)
    data = load_pickle_list(file_path)
    data = merge_pickle_list(data)
    data = fix_utf8(data)
    embeddings = get_embeddings(data)

    file_id = file.split(".")[0]
    title = titles[file_id]

    vectors = []
    for index, e in enumerate(embeddings):
        vectors.append(
            {
                "id": file_id + "_" + str(index),
                "values": e.embedding,
                "metadata": {
                    "text": data[index],
                    "rec_id": file_id,
                    "title": title,
                },
            }
        )

    upsert_vectors(vectors)
    with conn_pg.cursor() as cur:
            cur.execute(
                "UPDATE ali SET embedded_time = %s WHERE id = %s",
                (datetime.now(UTC), file_id),
            )
            conn_pg.commit()

    # logging.info(f"{file_id} embedding finished")

cur.close()
conn_pg.close()


# files_in_dir = os.listdir(dir)

# # Filter out files with ".docx" in their names
# docx_files_in_dir = [file for file in files_in_dir if ".docx" in file]

# # Remove ".docx.pkl" from the file names for further processing
# files_without_extension = [file.replace(".docx.pkl", "") for file in docx_files_in_dir]

# for file_without_extension in files_without_extension:

#     file_path = os.path.join(dir, file_without_extension + ".docx.pkl")

#     data = load_pickle_list(file_path)
#     data = merge_pickle_list(data)
#     data = fix_utf8(data)
#     embeddings = get_embeddings(data)

#     file_id = file_without_extension

#     vectors = []
#     fulltext_list = []
#     for index, e in enumerate(embeddings):
#         vectors.append(
#             {
#                 "id": uuid.uuid4().hex,
#                 "values": e.embedding,
#                 "metadata": {
#                     "text": data[index],
#                     "title": file_id,
#                 },
#             }
#         )
#         fulltext_list.append(
#                 {
#                     "text": data[index],
#                     "title": file_id,
#                 }
#             )

#     upsert_vectors(vectors)

#     n = len(fulltext_list)
#     for i in range(0, n, 500):
#         batch = fulltext_list[i : i + 500]
#         result = xata.records().bulk_insert("fulltext", {"records": batch})

#     logging.info(f"Embedding finished for file_id: {file_id}")
