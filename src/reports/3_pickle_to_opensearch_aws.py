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
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth


load_dotenv()

logging.basicConfig(
    filename="report_opensearch_aws.log",
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
                if key.endswith(".pkl"):
                    docs.append(key)
    return docs


def load_pickle_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    body = response["Body"].read()
    data = pickle.loads(body)
    return data


bucket_name = "tiangong"
prefix = "processed_docs/reports_pickle/"

# docs = list_all_objects(bucket_name, prefix)

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
reports_mapping = {
    "settings": {"analysis": {"analyzer": {"smartcn": {"type": "smartcn"}}}},
    "mappings": {
        "properties": {
            "rec_id": {"type": "keyword"},
            "text": {
                "type": "text",
                "analyzer": "smartcn",
            },
            "title": {
                "type": "text",
                "analyzer": "smartcn",
            },
            "organization": {
                "type": "text",
                "analyzer": "smartcn",
            },
            "release_date": {"type": "date", "format": "epoch_second"},
            "url":{"type": "text"}
        },
    },
}

if not client.indices.exists(index="reports"):
    print("Creating 'reports' index...")
    client.indices.create(index="reports", body=reports_mapping)


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


conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id, title, issuing_organization, release_date, url FROM reports"
    )
    records = cur.fetchall()

ids = [record[0] for record in records]
titles = {record[0]: record[1] for record in records}
organizations = {record[0]: record[2] for record in records}
release_dates = {record[0]: record[3] for record in records}
urls = {record[0]: record[4] for record in records}

keys = [str(id) + ".pkl" for id in ids]

for key in keys:
    data = load_pickle_from_s3(bucket_name, prefix + key)
    data = merge_pickle_list(data)
    data = fix_utf8(data)

    file_id = key.split(".")[0]
    title = titles[file_id]
    organization = "，".join(organizations[file_id])
    release_date = int(release_dates[file_id].timestamp())
    url = urls[file_id]

    fulltext_list = []
    for index, d in enumerate(data):
        fulltext_list.append(
            {"index": {"_index": "reports", "_id": file_id + "_" + str(index)}}
        )
        fulltext_list.append(
            {
                "text": data[index],
                "rec_id": file_id,
                "title": title,
                "organization": organization,
                "release_date": release_date,
                "url": url,
            }
        )
    n = len(fulltext_list)
    for i in range(0, n, 500):
        batch = fulltext_list[i : i + 500]
        client.bulk(body=batch)

    with conn_pg.cursor() as cur:
        cur.execute(
            "UPDATE reports SET fulltext_time = %s WHERE id = %s",
            (datetime.now(UTC), file_id),
        )
        conn_pg.commit()

    logging.info(f"Fulltext indexed for {file_id}")

conn_pg.close()
