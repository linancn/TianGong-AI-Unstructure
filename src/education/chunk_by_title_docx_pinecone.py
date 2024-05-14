import glob
import os
import re
import uuid
from ast import Dict, List

import pinecone
from dotenv import load_dotenv
from openai import OpenAI
from unstructured.chunking.title import chunk_by_title
from unstructured.documents.elements import CompositeElement, Table
from unstructured.partition.docx import partition_docx
from pinecone import Pinecone
import logging

load_dotenv()

client = OpenAI()

plugin_key = os.getenv("PLUGIN_KEY")
pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_environment = os.getenv("PINECONE_ENVIRONMENT")
pinecone_index = os.getenv("PINECONE_INDEX")

pc = Pinecone(api_key=pinecone_api_key)
index = pc.Index(pinecone_index)


def openai_embedding(text_list, source: str):
    keys_list = [list(d.keys())[0] for d in text_list]
    value_list = [list(d.values())[0] for d in text_list]
    response = client.embeddings.create(
        input=keys_list,
        # model="text-embedding-ada-002"
        model="text-embedding-3-large",
    )
    vectors = []
    for text, embedding in zip(value_list, response.data):
        vector = {
            "id": str(uuid.uuid4()),
            "values": embedding.embedding,
            "metadata": {"text": text, "source": source},
        }
        vectors.append(vector)
    return vectors


def upsert_vectors(vectors):
    try:
        index.upsert(
            vectors=vectors, batch_size=200, namespace="book", show_progress=False
        )
    except Exception as e:
        logging.error(e)


def process_in_batches(contents, batch_size=100):
    embedding_vectors = []
    for i in range(0, len(contents), batch_size):
        batch = contents[i : i + batch_size]
        embedding_vector = openai_embedding(
            text_list=batch, source=file_name_without_ext
        )
        upsert_vectors(embedding_vector)
        embedding_vectors.extend(embedding_vector)
    return embedding_vectors


def extract_text(file_name: str):
    elements = partition_docx(
        filename=file_name,
        multipage_sections=True,
        infer_table_structure=True,
        include_page_breaks=False,
    )

    chunks = chunk_by_title(
        elements=elements,
        multipage_sections=True,
        combine_text_under_n_chars=0,
        new_after_n_chars=None,
        max_characters=4096,
    )

    text_list = []

    for chunk in chunks:
        if isinstance(chunk, CompositeElement):
            text = chunk.text
            text_list.append(text)
        elif isinstance(chunk, Table):
            if text_list:
                text_list[-1] = text_list[-1] + "\n" + chunk.metadata.text_as_html
            else:
                text_list.append(chunk.hunk.metadata.text_as_html)
    result_list = []
    for text in text_list:
        split_text = text.split("\n\n", 1)
        if len(split_text) == 2:
            title, _ = split_text
        else:
            title = text
        result_list.append({title: text})
    return result_list


directory = "test"

for file_path in glob.glob(os.path.join(directory, "*.docx")):
    file_name = os.path.basename(file_path)
    file_name_without_ext = re.split(r"\.docx$", file_name)[0]

    contents = extract_text(file_path)

    embedding_vectors = process_in_batches(contents)
