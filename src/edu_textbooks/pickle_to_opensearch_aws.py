import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO
import arrow

import pandas as pd
from psycopg2 import pool
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


load_dotenv()

logging.basicConfig(
    filename="textbook_opensearch_aws.log",
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


def load_pickle_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    body = response["Body"].read()
    data = pickle.loads(body)
    return data


bucket_name = "tiangong"
prefix = "processed_docs/edu_textbooks_pickle/"
suffix = ".pkl"

client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    pool_maxsize=20,
    timeout=300,
)

# Create the index
textbook_mapping = {
    "settings": {"analysis": {"analyzer": {"smartcn": {"type": "smartcn"}}}},
    "mappings": {
        "properties": {
            "rec_id": {"type": "keyword"},
            "text": {"type": "text", "analyzer": "smartcn"},
            "title": {
                "type": "text",
                "analyzer": "smartcn",
            },
            "author": {
                "type": "text",
                "analyzer": "smartcn",
            },
            "publication_date": {"type": "date", "format": "epoch_second"},
            "isbn_number": {"type": "keyword"},
            "page_number": {
                "type": "keyword",
            },
        },
    },
}

if not client.indices.exists(index="textbooks"):
    client.indices.create(index="textbooks", body=textbook_mapping)
    print("'textbooks' index Created.")


def to_unix_timestamp(date_str: str) -> int:
    try:
        # Parse the date string using arrow
        date_obj = arrow.get(date_str)
        return int(date_obj.timestamp())
    except arrow.parser.ParserError:
        # If the parsing fails, return the current unix timestamp and log a warning
        return int(arrow.now().timestamp())


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


conn_pool = pool.SimpleConnectionPool(
    1,
    20,  # min and max number of connections
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pool.getconn() as conn_pg:
    with conn_pg.cursor() as cur:
        cur.execute(
            """SELECT id, title, authors, isbn_number,publish_time FROM edu_textbooks WHERE pdf_exist is NULL"""
        )
        records = cur.fetchall()

ids = [record[0] for record in records]
titles = {record[0]: record[1] for record in records}
authors = {record[0]: record[2] for record in records}
isbn_numbers = {record[0]: record[3] for record in records}
publish_times = {record[0]: record[4] for record in records}

files = [str(id) + ".pkl" for id in ids]

for file in files:
    try:
        data = load_pickle_from_s3(bucket_name, prefix + file)
        data = merge_pickle_list(data)
        data = fix_utf8(data)

        file_id = file.split(".")[0]
        title = titles[file_id]
        author = ", ".join(authors[file_id])
        isbn_number = isbn_numbers[file_id]
        publish_time = to_unix_timestamp(publish_times[file_id])

        fulltext_list = []
        for index, d in enumerate(data):
            fulltext_list.append(
                {"index": {"_index": "textbooks", "_id": file_id + "_" + str(index)}}
            )
            fulltext_list.append(
                {
                    "text": data[index][0],
                    "page_number": data[index][1],
                    "rec_id": file_id,
                    "title": title,
                    "author": author,
                    "isbn_number": isbn_number,
                    "publication_date": publish_time,
                }
            )
        n = len(fulltext_list)
        for i in range(0, n, 500):
            batch = fulltext_list[i : i + 500]
            client.bulk(body=batch)

        # Get a connection from the pool
        conn_pg = conn_pool.getconn()
        try:
            with conn_pg.cursor() as cur:
                cur.execute(
                    "UPDATE edu_textbooks SET fulltext_time = %s WHERE id = %s",
                    (datetime.now(UTC), file_id),
                )
                conn_pg.commit()
                logging.info(f"Updated {file_id} in the database.")
        finally:
            # Release the connection back to the pool
            conn_pool.putconn(conn_pg)
    except Exception:
        logging.error(f"Error processing {file_id}")
# Close the connection pool
conn_pool.closeall()
