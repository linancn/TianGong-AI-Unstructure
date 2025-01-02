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

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


load_dotenv()

logging.basicConfig(
    filename="journal_opensearch_aws.log",
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

bucket_name = "tiangong"
prefix = "processed_docs/journal_pickle/"
suffix = ".pkl"


def load_pickle_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    body = response["Body"].read()
    data = pickle.loads(body)
    return data


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
sci_mapping = {
    "mappings": {
        "properties": {
            "text": {"type": "text"},
            "doi": {
                "type": "keyword",
            },
            "journal": {
                "type": "keyword",
            },
            "date": {"type": "date", "format": "epoch_second"},
        },
    },
}

if not client.indices.exists(index="sci"):
    client.indices.create(index="sci", body=sci_mapping)
    print("'sci' index Created.")


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
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append(cleaned_str)
    return cleaned_list


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
            "SELECT id,doi,journal,date FROM journals WHERE upload_time IS NOT NULL AND fulltext_time IS NULL"
        )
        records = cur.fetchall()

conn_pg = conn_pool.getconn()

for record in records:
    try:
        data = load_pickle_from_s3(bucket_name, prefix + record[0] + ".pkl")
        data = merge_pickle_list(data)
        data = fix_utf8(data)

        file_id = record[1]
        journal = record[2]
        date = to_unix_timestamp(record[3])

        fulltext_list = []
        for index, d in enumerate(data):
            fulltext_list.append(
                {"index": {"_index": "sci", "_id": file_id + "_" + str(index)}}
            )
            fulltext_list.append(
                {
                    "text": data[index],
                    "doi": file_id,
                    "journal": journal,
                    "date": date,
                }
            )
        n = len(fulltext_list)
        for i in range(0, n, 500):
            batch = fulltext_list[i : i + 500]
            client.bulk(body=batch)

            with conn_pg.cursor() as cur:
                cur.execute(
                    "UPDATE journals SET fulltext_time = %s WHERE doi = %s",
                    (datetime.now(UTC), file_id),
                )
                conn_pg.commit()
                logging.info(f"Updated {file_id} in the database.")

    except Exception:
        logging.error(f"Error processing {record[0]}")

conn_pool.putconn(conn_pg)
# Close the connection pool
conn_pool.closeall()
