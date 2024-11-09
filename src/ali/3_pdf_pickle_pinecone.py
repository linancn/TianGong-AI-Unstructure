import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO

import pandas as pd
import psycopg2
import tiktoken
from openai import OpenAI
from bs4 import BeautifulSoup
from pinecone import Pinecone
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed


load_dotenv()

logging.basicConfig(
    filename="ali_pdf_pinecone.log",
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
        cleaned_str = original_str[0].replace("\ufffd", " ")
        cleaned_list.append([cleaned_str, original_str[1]])
    return cleaned_list


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_embeddings(items, model="text-embedding-3-small"):
    text_list = [item[0] for item in items]
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
        print(e)


def load_pickle_list(file_path):
    with open(file_path, "rb") as f:
        data = pickle.load(f)
    # clean_data = [item[0] for item in data if isinstance(item, tuple)]

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
        if num_tokens_from_string(d[0]) > 8100:
            soup = BeautifulSoup(d[0], "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    if table_content:  # check if table_content is not empty
                        result.append([table_content, d[1]])
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append([str(soup), d[1]])
                    except Exception as e:
                        logging.error(f"Error splitting dataframe table: {e}")
        elif num_tokens_from_string(d[0]) < 15:
            temp += d[0] + " "
        else:
            result.append([(temp + d[0]), d[1]])
            temp = ""
    if temp:
        result.append([temp, d[1]])

    return result


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="internal_use", show_progress=False
        )
    except Exception as e:
        logging.error(e)


conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id, title, tag FROM internal_use WHERE file_type = '.pdf'"
    )
    records = cur.fetchall()

ids = [record[0] for record in records]
titles = {record[0]: record[1] for record in records}
tags = {record[0]: record[2] for record in records}

files = [str(id) + ".pdf.pkl" for id in ids]

dir = "processed_docs/ali_pickle"

# update_data = []

for file in files:
    file_path = os.path.join(dir, file)
    data = load_pickle_list(file_path)
    data = merge_pickle_list(data)
    data = fix_utf8(data)
    embeddings = get_embeddings(data)

    file_id = file.split(".")[0]
    title = titles[file_id]
    tag = tags[file_id]

    vectors = []
    for index, e in enumerate(embeddings):
        vectors.append(
            {
                "id": file_id + "_" + str(index),
                "values": e.embedding,
                "metadata": {
                    "text": data[index][0],
                    "rec_id": file_id,
                    "title": title,
                    "tag": tag,
                },
            }
        )

    upsert_vectors(vectors)
    # Get a connection from the pool
    with conn_pg.cursor() as cur:
        cur.execute(
            "UPDATE internal_use SET embedded_time = %s WHERE id = %s",
            (datetime.now(UTC), file_id),
        )
        conn_pg.commit()
        # logging.info(f"Updated {file_id} in the database.")


# Close the connection pool
cur.close()
conn_pg.close()