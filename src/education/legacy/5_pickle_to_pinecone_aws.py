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
import boto3
from openai import OpenAI
from pinecone import Pinecone
from opensearchpy import AWSV4SignerAuth
from tenacity import retry, stop_after_attempt, wait_fixed


load_dotenv()

logging.basicConfig(
    filename="education_pinecone.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)

host = os.environ.get("AWS_OPENSEARCH_URL")
region = "us-east-1"

service = "aoss"
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

s3_client = boto3.client("s3")


def list_all_objects(bucket_name, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    docs = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                if key != prefix:
                    docs.append(key)
    return docs


def load_pickle_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    body = response["Body"].read()
    data = pickle.loads(body)
    return data


bucket_name = "tiangong"
prefix = "processed_docs/education_pickle/"

# docs = list_all_objects(bucket_name, prefix)

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
        d = d["text"]
        if num_tokens_from_string(d) > 8100:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    if table_content:  # check if table_content is not empty
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


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_embeddings(text_list, model="text-embedding-3-small"):
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
        logging.error(e)


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="education", show_progress=False
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
        "SELECT id, course, file_type, name, chapter_number FROM edu_meta WHERE upload_time IS NOT NULL and embedding_time IS NULL"
    )
    records = cur.fetchall()

ids = [record[0] for record in records]
courses = {record[0]: record[1] for record in records}
file_types = {record[0]: record[2] for record in records}
names = {record[0]: record[3] for record in records}
chapter_numbers = {record[0]: record[4] for record in records}

keys = [str(id) + file_types[id] + ".pkl" for id in ids]

update_data = []

for key in keys:
    data = load_pickle_from_s3(bucket_name, prefix + key)
    data = merge_pickle_list(data)
    data = fix_utf8(data)
    embeddings = get_embeddings(data)

    file_id = key.split(".")[0]
    course = courses[file_id]
    name = names[file_id]
    chapter_number = chapter_numbers[file_id]

    vectors = []
    for index, e in enumerate(embeddings):
        vectors.append(
            {
                "id": file_id + "_" + str(index),
                "values": e.embedding,
                "metadata": {
                    "text": data[index],
                    "rec_id": file_id,
                    "course": course,
                    "name": name,
                    "chapter_number": chapter_number,
                },
            }
        )

    upsert_vectors(vectors)
    update_data.append((datetime.now(UTC), file_id))


def chunk_list(data, chunk_size):
    """Yield successive chunk_size chunks from data."""
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


chunk_size = 100


with conn_pg.cursor() as cur:
    total_chunks = len(update_data) // chunk_size + (
        1 if len(update_data) % chunk_size > 0 else 0
    )
    for i, chunk in enumerate(chunk_list(update_data, chunk_size), start=1):
        cur.executemany(
            "UPDATE edu_meta SET embedding_time = %s WHERE id = %s",
            chunk,
        )
        conn_pg.commit()
        logging.info(
            f"Updated chunk {i}/{total_chunks}, {len(chunk)} records in this chunk."
        )

conn_pg.close()
