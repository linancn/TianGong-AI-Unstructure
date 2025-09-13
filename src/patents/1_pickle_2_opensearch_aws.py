import logging
import numpy
import os
import pickle

import tiktoken
from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from tenacity import retry, stop_after_attempt, wait_fixed

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

load_dotenv()

logging.basicConfig(
    filename="patents_2_opensearch.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

tokenizer = tiktoken.get_encoding("cl100k_base")

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

# Create the index
patent_mapping = {
    "mappings": {
        "properties": {
            "abstract": {"type": "text"},
            "title": {
                "type": "text",
            },
            "country": {
                "type": "keyword",
            },
            "url": {
                "type": "text",
            },
            "publication_date": {"type": "date", "format": "epoch_second"},
        },
    },
}

if not client.indices.exists(index="patents"):
    client.indices.create(index="patents", body=patent_mapping)
    print("'patents' index Created.")

s3_client = boto3.client("s3")


def load_pickle_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    body = response["Body"].read()
    data = pickle.loads(body)
    return data


bucket_name = "tiangong"
df = load_pickle_from_s3(bucket_name, "patents/patent_241113.pkl")

# df = df.head(10000)
df = df.map(lambda x: None if x == "" else x).dropna()
df = df.reset_index(drop=True)

# print(df.head())
bulk_count = 0
fulltext_list = []
for i, row in df.iterrows():
    # logging.info(i)
    fulltext_list.append(
        {"index": {"_index": "patents", "_id": row["publication_number"]}}
    )
    fulltext_list.append(
        {
            "abstract": row["abstract"],
            "title": row["title"],
            "url": row["url"],
            "country": row["country"],
            "publication_date": int(row["publication_date"]),
        }
    )
    if len(fulltext_list) >= 1000:
        client.bulk(body=fulltext_list)
        fulltext_list = []
        bulk_count += 1
        print(f"Bulk {bulk_count} inserted")

if fulltext_list:
    client.bulk(body=fulltext_list)
