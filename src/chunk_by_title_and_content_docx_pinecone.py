import glob
import os
import re
import uuid

import pinecone
from dotenv import load_dotenv
from openai import OpenAI
from unstructured.chunking.title import chunk_by_title
from unstructured.documents.elements import CompositeElement, Table
from unstructured.partition.docx import partition_docx

load_dotenv()

client = OpenAI()

plugin_key = os.getenv("PLUGIN_KEY")
pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_environment = os.getenv("PINECONE_ENVIRONMENT")
pinecone_index = os.getenv("PINECONE_INDEX")

pinecone.init(api_key=pinecone_api_key, environment=pinecone_environment)
index = pinecone.Index(pinecone_index)


def openai_embedding(text_list: list[str], source: str):
    response = client.embeddings.create(
        input=text_list,
        model="text-embedding-ada-002",
    )
    vectors = []
    for text, embedding in zip(text_list, response.data):
        vector = {
            "id": str(uuid.uuid4()),
            "values": embedding.embedding,
            "metadata": {"text": text, "source": source},
        }
        vectors.append(vector)
    return vectors


def process_in_batches(contents, batch_size=100):
    embedding_vectors = []
    for i in range(0, len(contents), batch_size):
        batch = contents[i : i + batch_size]
        embedding_vector = openai_embedding(
            text_list=batch, source=file_name_without_ext
        )
        index.upsert(
            vectors=embedding_vector,
        )
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
                text_list[-1] = text_list[-1] + "\n" + chunk.text
            else:
                text_list.append(chunk.text)

    return text_list


directory = "water"

for file_path in glob.glob(os.path.join(directory, "*.docx")):
    file_name = os.path.basename(file_path)
    file_name_without_ext = re.split(r"\.docx$", file_name)[0]

    contents = extract_text(file_path)

    embedding_vectors = process_in_batches(contents)
