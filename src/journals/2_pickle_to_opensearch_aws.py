import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO
from urllib.parse import unquote

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


# def list_all_objects(bucket_name, prefix):
#     paginator = s3_client.get_paginator("list_objects_v2")
#     page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

#     docs = []
#     for page in page_iterator:
#         if "Contents" in page:
#             for obj in page["Contents"]:
#                 key = obj["Key"]
#                 if key.endswith(".pkl"):
#                     docs.append(key)
#     return docs


def list_all_objects(bucket_name, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    docs = []
    count = 0
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                if count >= 10:
                    break
                key = obj["Key"]
                if key.endswith(".pkl"):
                    docs.append(key)
                    count += 1
        if count >= 10:
            break
    return docs


def load_pickle_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    body = response["Body"].read()
    data = pickle.loads(body)
    return data


bucket_name = "tiangong"
prefix = "processed_docs/journal_pickle/"
suffix = ".pdf.pkl"


def extract_doi_from_path(path):
    doi = path[len(prefix) :][: -len(suffix)]
    return doi


docs = list_all_objects(bucket_name, prefix)


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


def get_files_before(directory, target_time):
    old_files = set()
    # 获取指定时间的时间戳
    target_timestamp = target_time

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            # 获取文件的修改时间
            stat = os.stat(file_path)
            file_ctime = stat.st_mtime
            if file_ctime < target_timestamp:
                relative_path = os.path.relpath(file_path, directory)
                old_files.add(relative_path)
    return old_files

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
            "SELECT doi,journal,date FROM journals WHERE upload_time IS NOT NULL AND fulltext_time IS NULL"
        )
        records = cur.fetchall()

dois = [record[0] for record in records]
journals = {record[0]: record[1] for record in records}
dates = {record[0]: record[2] for record in records}

docs_dois = [unquote(unquote(extract_doi_from_path(doc))) for doc in docs]

df = pd.DataFrame({"doi": docs_dois, "path": docs})

for index, row in df.iterrows():
    try:
        data = load_pickle_from_s3(bucket_name, row['path'])
        data = merge_pickle_list(data)
        data = fix_utf8(data)
        embeddings = get_embeddings(data)

        file_id = row['doi']
        journal = journals[file_id]
        date = int(dates[file_id].timestamp())

        fulltext_list = []
        for index, d in enumerate(data):
            fulltext_list.append(
                {"index": {"_index": "edu"}}
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

        # Get a connection from the pool
        conn_pg = conn_pool.getconn()
        try:
            with conn_pg.cursor() as cur:
                cur.execute(
                    "UPDATE journal SET fulltext_time = %s WHERE doi = %s",
                    (datetime.now(UTC), file_id),
                )
                conn_pg.commit()
                logging.info(f"Updated {file_id} in the database.")
        finally:
            # Release the connection back to the pool
            conn_pool.putconn(conn_pg)
    except Exception:
        logging.error(f"Error processing {row['path']}")
# Close the connection pool
conn_pool.closeall()
