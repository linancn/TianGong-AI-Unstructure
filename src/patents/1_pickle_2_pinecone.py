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
from opensearchpy import AWSV4SignerAuth

load_dotenv()

logging.basicConfig(
    filename="patents_2_pinecone.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

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

# with open("docs/patents/patent_241113.pkl", "rb") as f:
#     df = pickle.load(f)

df = load_pickle_from_s3(bucket_name, "patents/patent_241113.pkl")

# df = df.head(10000)
df = df.map(lambda x: None if x == "" else x).dropna()
df = df.reset_index(drop=True)

# print(df.head())

client = OpenAI()
pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY_US_EAST_1"))
idx = pc.Index(os.environ.get("PINECONE_SERVERLESS_INDEX_NAME_US_EAST_1"))


# def to_unix_timestamp(date_str: str) -> int:
#     """
#     Convert a date string to a unix timestamp (seconds since epoch).

#     Args:
#         date_str: The date string to convert.

#     Returns:
#         The unix timestamp corresponding to the date string.

#     If the date string cannot be parsed as a valid date format, returns the current unix timestamp and prints a warning.
#     """
#     try:
#         # Parse the date string using arrow
#         date_obj = arrow.get(date_str)
#         return int(date_obj.timestamp())
#     except arrow.parser.ParserError:
#         # If the parsing fails, return the current unix timestamp and log a warning
#         return int(arrow.now().timestamp())


tokenizer = tiktoken.get_encoding("cl100k_base")


def limit_token_length(abstract):
    encoded = tokenizer.encode(abstract)
    if len(encoded) > 8192:
        tokens = encoded[:8192]
        truncated_text = tokenizer.decode(tokens)
        return truncated_text
    else:
        return abstract


df["abstract"] = df["abstract"].apply(limit_token_length)


df["values"] = [None] * len(df)
df["values"] = df["values"].astype("object")


# @retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def get_embeddings(input):
    try:
        return client.embeddings.create(
            input=input, model="text-embedding-3-small"
        ).data
    except Exception as e:
        logging.error(e)


# @retry(wait=wait_fixed(3), stop=stop_after_attempt(10))


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="patent", show_progress=False
        )
    except Exception as e:
        logging.error(e)


for i in range(0, len(df), 1000):
    logging.info(i)
    embeddings = get_embeddings(df["abstract"][i : i + 1000])
    for j in range(len(embeddings)):
        df.at[i + j, "values"] = embeddings[j].embedding

    vectors = []
    for j in range(len(embeddings)):
        vectors.append(
            {
                "id": df.at[i + j, "publication_number"],
                "values": df.at[i + j, "values"],
                "metadata": {
                    "title": df.at[i + j, "title"],
                    "abstract": df.at[i + j, "abstract"],
                    "url": df.at[i + j, "url"],
                    "country": df.at[i + j, "country"],
                    "publication_date": int(df.at[i + j, "publication_date"]),
                },
            }
        )

    upsert_vectors(vectors)

    df["values"] = None
    del embeddings
    del vectors

# with open("patents_data/20240208/patents_pinocone_with_vectors.pkl", "wb") as f:
#     pickle.dump(df, f)
