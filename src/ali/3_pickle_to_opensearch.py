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

# from supabase import create_client, Client

load_dotenv()

logging.basicConfig(
    filename="epr-fulltext.log",
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
client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    pool_maxsize=20,
    timeout=300,
)

internal_use_mapping = {
    "settings": {"analysis": {"analyzer": {"smartcn": {"type": "smartcn"}}}},
    "mappings": {
        "properties": {
            "text": {
                "type": "text",
                "analyzer": "smartcn",
            },
            "rec_id": {"type": "keyword"},
            "tag": {"type": "keyword"},
        },
    },
}

if not client.indices.exists(index="internal_use"):
    print("Creating 'internal_use' index...")
    client.indices.create(index="internal_use", body=internal_use_mapping)


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
        if num_tokens_from_string(d["text"]) > 8100:
            soup = BeautifulSoup(d["text"], "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    if table_content:  # check if table_content is not empty
                        result.append([table_content, d["page_number"]])
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append([str(soup), d["page_number"]])
                    except Exception as e:
                        logging.error(f"Error splitting dataframe table: {e}")
        elif num_tokens_from_string(d["text"]) < 15:
            temp += d["text"] + " "
        else:
            result.append([(temp + d["text"]), d["page_number"]])
            temp = ""
    if temp:
        result.append([temp, d["page_number"]])

    return result


conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute("SELECT id, title,tag FROM internal_use WHERE tag = 'epr'")
    records = cur.fetchall()

ids = [record[0] for record in records]
titles = {record[0]: record[1] for record in records}
tags = {record[0]: record[2] for record in records}

files = [str(id) + ".pkl" for id in ids]

dir = "temp/epr"

# update_data = []

for file in files:
    file_path = os.path.join(dir, file)
    if not os.path.exists(os.path.join(dir, file)):
        logging.error(f"File {file} does not exist in directory {dir}. Skipping.")
        continue
    
    # try:
    data = load_pickle_list(file_path)
    data = merge_pickle_list(data)
    data = fix_utf8(data)

    file_id = file.split(".")[0]
    title = titles[file_id]
    tag = tags[file_id]


    fulltext_list = []
    for index, d in enumerate(data):
        fulltext_list.append(
            {"index": {"_index": "internal_use", "_id": file_id + "_" + str(index)}}
        )
        fulltext_list.append(
            {
                "text": data[index][0],
                "rec_id": file_id,
                "title": title,
                "tag": tag,
            }
        )
    n = len(fulltext_list)
    for i in range(0, n, 1000):
        batch = fulltext_list[i : i + 1000]
        client.bulk(body=batch)

    with conn_pg.cursor() as cur:
        cur.execute(
            "UPDATE internal_use SET fulltext_time = %s WHERE id = %s",
            (datetime.now(UTC), file_id),
        )
        conn_pg.commit()
    # except Exception as e:
    #     logging.error(f"Error processing file {file_path}: {e}")
    #     continue

cur.close()
conn_pg.close()

