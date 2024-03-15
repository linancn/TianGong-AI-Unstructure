import logging
import os
import pickle

import arrow
import tiktoken
from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

logging.basicConfig(
    filename="patents_2_pinecone.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

with open("patents_data/20240208/patents_pinocone.pkl", "rb") as f:
    df = pickle.load(f)

# 根据publication_number去重，只保留第一个
# df = df.drop_duplicates(subset='publication_number', keep='first')

# print(df.head())

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


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="patent", show_progress=False
        )
    except Exception as e:
        logging.error(e)


for i in range(3630000, len(df), 1000):
    logging.info(i)
    embeddings = get_embeddings(df["abstract"][i : i + 1000])
    for j in range(len(embeddings)):
        df.at[i + j, "values"] = embeddings[j].embedding

    vectors = []
    for j in range(i, i + 1000):
        vectors.append(
            {
                "id": df.at[j, "publication_number"],
                "values": df.at[j, "values"],
                "metadata": {
                    "title": df.at[j, "title"],
                    "abstract": df.at[j, "abstract"],
                    "url": df.at[j, "url"],
                    "country": df.at[j, "country"],
                    "publication_date": to_unix_timestamp(df.at[j, "publication_date"]),
                },
            }
        )

    upsert_vectors(vectors)

    df["values"] = None
    del embeddings
    del vectors

# with open("patents_data/20240208/patents_pinocone_with_vectors.pkl", "wb") as f:
#     pickle.dump(df, f)
