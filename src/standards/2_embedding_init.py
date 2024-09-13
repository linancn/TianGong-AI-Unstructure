from io import StringIO
import logging
import os
import pickle
from datetime import UTC, datetime

import arrow
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
    filename="standard_embedding.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

client = OpenAI()

pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY"))
idx = pc.Index(os.environ.get("PINECONE_SERVERLESS_INDEX_NAME"))

def to_unix_timestamp(date_str: str) -> int:
    """
    Convert a date string to a unix timestamp (seconds since epoch).

    Args:
        date_str: The date string to convert.

    Returns:
        The unix timestamp corresponding to the date string.

    If the date string cannot be parsed as a valid date format, returns the current unix timestamp and prints a warning.
    """
    try:
        # Parse the date string using arrow
        date_obj = arrow.get(date_str)
        return int(date_obj.timestamp())
    except arrow.parser.ParserError:
        # If the parsing fails, return the current unix timestamp and log a warning
        return int(arrow.now().timestamp())
        # If the parsing fails, return the curr

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


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="standard", show_progress=False
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
        "SELECT id, title, standard_number, issuing_organization, effective_date FROM standards WHERE uploaded_time IS NOT NULL AND embedded_time IS NULL AND expiration_date IS NULL"
    )
    records = cur.fetchall()

ids = [record[0] for record in records]
titles = {record[0]: record[1] for record in records}
standard_numbers = {record[0]: record[2] for record in records}
issuing_organization = {record[0]: record[3] for record in records}
effective_date = {record[0]: record[4] for record in records}

files = [str(id) + ".pkl" for id in ids]


dir = "standards_pickle"

for file in files:

    try:
        file_path = os.path.join(dir, file)
        data = load_pickle_list(file_path)
        data = merge_pickle_list(data)
        data = fix_utf8(data)
        embeddings = get_embeddings(data)

        file_id = file.split(".")[0]
        organization = issuing_organization[file_id]
        date = effective_date[file_id]

        vectors = []
        fulltext_list = []
        for index, e in enumerate(embeddings):
            # fulltext_list.append(
            #     {
            #         "sort_number": index,
            #         "text": data[index],
            #         "standard_id": file_id,
            #     }
            # )
            vectors.append(
                {
                    "id": file_id + "_" + str(index),
                    "values": e.embedding,
                    "metadata": {
                        "text": data[index],
                        "rec_id": file_id,
                        "title": titles[file_id],
                        "standard_number": standard_numbers[file_id],
                        "organization": ",".join(organization),
                        "date": to_unix_timestamp(date),
                    },
                }
            )

        # n = len(fulltext_list)
        # for i in range(0, n, 500):
        #     batch = fulltext_list[i : i + 500]
        #     result = xata.records().bulk_insert("standard_fulltext", {"records": batch})

        # logging.info(
        #     f"{file_id} fulltext insert finished, with status_code: {result.status_code}"
        # )

        upsert_vectors(vectors)

        with conn_pg.cursor() as cur:
            cur.execute(
                "UPDATE standards SET embedded_time = %s WHERE id = %s",
                (datetime.now(UTC), file_id),
            )
            conn_pg.commit()

        logging.info(f"{file_id} embedding finished")

    except Exception as e:
        logging.error(e)
        continue
