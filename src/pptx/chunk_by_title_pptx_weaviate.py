import glob
import os
import re

import weaviate
import weaviate.classes as wvc
from dotenv import load_dotenv
from unstructured.chunking.title import chunk_by_title
from unstructured.documents.elements import CompositeElement, Table
from unstructured.partition.pptx import partition_pptx
from weaviate.config import AdditionalConfig

load_dotenv()


def extract_text(file_name: str):
    elements = partition_pptx(
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
        result_list.append({title: text})
    return result_list


def split_chunks(text_list: list, source: str):
    chunks = []
    for text in text_list:
        for key, value in text.items():
            chunks.append({"question": key, "answer": value, "source": source})
    return chunks


directory = "test"

for file_path in glob.glob(os.path.join(directory, "*.pptx")):
    file_name = os.path.basename(file_path)
    file_name_without_ext = re.split(r"\.pptx$", file_name)[0]

    contents = extract_text(file_path)
    w_chunks = split_chunks(text_list=contents, source=file_name_without_ext)


# w_client = weaviate.connect_to_local(
#     host="localhost", additional_config=AdditionalConfig(timeout=(600, 800))
# )

# try:
#     collection = w_client.collections.create(
#         name="Water",
#         vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_transformers(),
#     )

#     directory = "water"

#     for file_path in glob.glob(os.path.join(directory, "*.docx")):
#         file_name = os.path.basename(file_path)
#         file_name_without_ext = re.split(r"\.docx$", file_name)[0]

#         contents = extract_text(file_path)
#         w_chunks = split_chunks(text_list=contents, source=file_name_without_ext)

#         questions = w_client.collections.get(name="Water")
#         questions.data.insert_many(w_chunks)

#     # w_client.collections.delete(name="water")

# finally:
#     w_client.close()
