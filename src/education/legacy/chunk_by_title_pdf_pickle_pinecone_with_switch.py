import logging
import os
import pickle
from datetime import UTC, datetime
from io import StringIO

import pandas as pd
import tiktoken
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from tenacity import retry, stop_after_attempt, wait_fixed
from xata import XataClient

load_dotenv()

logging.basicConfig(
    filename="education_pdf_embedding.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)

client = OpenAI()

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_DOCS_DB_URL")

xata = XataClient(
    api_key=xata_api_key,
    db_url=xata_db_url,
)

pc = Pinecone(api_key=os.getenv("PINECONE_SERVERLESS_API_KEY"))
idx = pc.Index(os.getenv("PINECONE_SERVERLESS_INDEX_NAME"))


def fetch_all_records(xata, table_name, columns, filter, page_size=1000):
    all_records = []
    cursor = None
    more = True

    while more:
        page = {"size": page_size}
        if not cursor:
            results = xata.data().query(
                table_name,
                {
                    "page": page,
                    "columns": columns,
                    "filter": filter,
                },
            )
        else:
            page["after"] = cursor
            results = xata.data().query(
                table_name,
                {
                    "page": page,
                    "columns": columns,
                },
            )

        all_records.extend(results["records"])
        cursor = results["meta"]["page"]["cursor"]
        more = results["meta"]["page"]["more"]

    return all_records


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(string))


def fix_utf8(original_list):
    cleaned_list = []
    for item in original_list:
        if isinstance(item, tuple):
            if len(item) == 2:
                title, original_str = item
            elif len(item) == 3:
                title, original_str, page = item
            else:
                continue
            cleaned_str = original_str.replace("\ufffd", " ")
            cleaned_list.append(
                (title, cleaned_str, page) if len(item) == 3 else (title, cleaned_str)
            )
        elif isinstance(item, str):
            cleaned_str = item.replace("\ufffd", " ")
            cleaned_list.append(cleaned_str)
        else:
            logging.error(f"Unexpected item type: {type(item)} in item: {item}")
    return cleaned_list


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_embeddings(items, model="text-embedding-3-small"):
    text_list = [
        item[1] if isinstance(item, tuple) and len(item) >= 2 else item
        for item in items
    ]
    try:
        text_list = [text.replace("\n\n", " ").replace("\n", " ") for text in text_list]
        length = len(text_list)
        results = []

        for i in range(0, length, 1000):
            result = client.embeddings.create(
                input=text_list[i : i + 1000], model=model
            ).data
            logging.info(
                f'HTTP Request: POST https://api.openai.com/v1/embeddings "HTTP/1.1 200 OK"'
            )
            results += result
        return results

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


def merge_pickle_list_with_page_numbers(data):
    temp = ""
    result = []
    for item in data:
        if isinstance(item, tuple):
            if len(item) == 3:
                title, d, page = item
            elif len(item) == 2:
                d, page = item
                title = "Default Title"
            else:
                continue
        else:
            d, page, title = item, None, "Default Title"

        if num_tokens_from_string(d) > 8100:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    result.append((title, table_content, page))
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append((title, str(soup), page))
                    except Exception as e:
                        logging.error(f"Error splitting dataframe table: {e}")
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append((title, temp + d, page))
            temp = ""
    if temp:
        result.append((title, temp, page))

    return result


def merge_pickle_list_without_page_numbers(data):
    temp = ""
    result = []
    for item in data:
        if isinstance(item, tuple):
            if len(item) == 2:
                title, d = item
            else:
                continue
        else:
            d, title = item, "Default Title"

        if num_tokens_from_string(d) > 8100:
            soup = BeautifulSoup(d, "html.parser")
            tables = soup.find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    result.append((title, table_content))
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append((title, str(soup)))
                    except Exception as e:
                        logging.error(f"Error splitting dataframe table: {e}")
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append((title, temp + d))
            temp = ""
    if temp:
        result.append((title, temp))

    return result


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10))
def upsert_vectors(vectors):
    try:
        idx.upsert(
            vectors=vectors, batch_size=200, namespace="education", show_progress=False
        )
    except Exception as e:
        logging.error(f"Error upserting vectors: {e}")
        raise


table_name = "education"
columns = ["id", "course", "embedding_time"]
filter = {"$all": [{"$notExists": "embedding_time"}]}

all_records = fetch_all_records(xata, table_name, columns, filter)

ids = [record["id"] for record in all_records]

dir = "education_pickle"

files_in_dir = os.listdir(dir)

# Set this to True if the original pickle files include page number information
include_page_numbers = False

if include_page_numbers:
    # Filter out files with ".pdf_pn" in their names
    pdf_files_in_dir = [file for file in files_in_dir if ".pdf_pn" in file]
    file_extension_to_remove = ".pdf_pn.pkl"
else:
    # Filter out files with ".pdf" in their names and without ".pdf_pn"
    pdf_files_in_dir = [
        file for file in files_in_dir if ".pdf" in file and ".pdf_pn" not in file
    ]
    file_extension_to_remove = ".pdf.pkl"

# Remove the specified extension from the file names for further processing
files_without_extension = [
    file.replace(file_extension_to_remove, "") for file in pdf_files_in_dir
]

for file_without_extension in files_without_extension:
    try:
        record_id = file_without_extension
        record = next(
            (record for record in all_records if record["id"] == record_id), None
        )
        if record:
            if "embedding_time" in record and record["embedding_time"] is not None:
                continue
            file_path = os.path.join(
                dir, file_without_extension + file_extension_to_remove
            )

            data = load_pickle_list(file_path)

            if include_page_numbers:
                data = merge_pickle_list_with_page_numbers(data)
            else:
                data = merge_pickle_list_without_page_numbers(data)

            data = fix_utf8(data)

            embeddings = get_embeddings(data)

            vectors = []
            for index, e in enumerate(embeddings):
                if include_page_numbers:
                    page_info = (
                        data[index][2]
                        if isinstance(data[index], tuple) and len(data[index]) == 3
                        else None
                    )
                    text_with_page = (
                        f"Page {page_info}: {data[index][1]}"
                        if page_info
                        else (
                            data[index][1]
                            if isinstance(data[index], tuple)
                            else data[index]
                        )
                    )
                else:
                    text_with_page = (
                        data[index][1]
                        if isinstance(data[index], tuple)
                        else data[index]
                    )

                if text_with_page.strip() == "":  # Skip empty texts
                    continue

                vectors.append(
                    {
                        "id": record_id + "_" + str(index),
                        "values": e.embedding,
                        "metadata": {
                            "text": text_with_page,
                            "rec_id": record_id,
                            "course": record["course"],
                        },
                    }
                )

            upsert_vectors(vectors)

            xata.records().update(
                "education",
                record_id,
                {"embedding_time": datetime.now(UTC).isoformat()},
            )
            logging.info(f"Embedding finished for file_id: {record_id}")

    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        continue
