import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO

import boto3
import pandas as pd
import psycopg2
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from pinecone import Pinecone
from requests_aws4auth import AWS4Auth
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

logging.basicConfig(
    filename="esg_embedding.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)

client = OpenAI()

region = "us-east-1"
service = "aoss"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token,
)

opensearch_client = OpenSearch(
    hosts=[{"host": os.environ.get("OPENSEARCH_ESG_URL"), "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=300,
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
    for item in original_list:
        original_str, page = item
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append((cleaned_str, page))
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
    for item in data:
        d, page = item
        if num_tokens_from_string(d) > 8100:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    if table_content:  # 确保表格内容不为空
                        result.append((table_content, page))
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append((str(soup), page))
                    except Exception as e:
                        logging.error(e)
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append(((temp + d), page))
            temp = ""
    if temp:
        result.append((temp, page))

    return result


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="esg", show_progress=False
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
        "SELECT id FROM esg_meta WHERE uploaded_time IS NOT NULL AND embedded_time IS NULL"
    )
    records = cur.fetchall()

ids = [record[0] for record in records]

files = [id + ".pkl" for id in ids]

dir = "esg_pickle"


for file in files:

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
                    "_op_type": "index",
                    "_index": "esg",
                    "_id": file_id + "_" + str(index),
                    "_source": {
                        "pageNumber": data[index][1],
                        "text": data[index][0],
                        "reportId": file_id,
                    },
                }
            )
            vectors.append(
                {
                    "id": file_id + "_" + str(index),
                    "values": e.embedding,
                    "metadata": {
                        "text": data[index][0],
                        "rec_id": file_id,
                        "page_number": data[index][1],
                    },
                }
            )

        n = len(fulltext_list)
        for i in range(0, n, 500):
            batch = fulltext_list[i : i + 500]
            helpers.bulk(opensearch_client, batch)

        logging.info(f"{file_id} fulltext insert finished.")

        upsert_vectors(vectors)

        with conn_pg.cursor() as cur:
            cur.execute(
                "UPDATE esg_meta SET embedded_time = %s WHERE id = %s",
                (datetime.now(UTC), file_id),
            )
            conn_pg.commit()

        logging.info(f"{file_id} embedding finished")

    except Exception as e:
        logging.error(e)
        continue

    finally:
        cur.close()
        conn_pg.close()
