import glob
import os
import pickle
import concurrent.futures
from unstructured.partition.docx import partition_docx
from unstructured.chunking.title import chunk_by_title
from unstructured.documents.elements import CompositeElement, Table


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
        max_characters=4000,
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
                text_list.append(chunk.metadata.text_as_html)
    return text_list


def merge_title_blocks(text_list):
    merged_list = []
    title_buffer = []

    for text in text_list:
        stripped_text = text.strip()
        if stripped_text and (
            not stripped_text.endswith(".") and not "\n" in stripped_text
        ):  # This is a title block
            title_buffer.append(stripped_text)
        else:
            if title_buffer:
                merged_title = "\n".join(title_buffer)
                merged_list.append(merged_title + "\n" + text)
                title_buffer = []
            else:
                merged_list.append(text)

    if title_buffer:
        if merged_list:
            merged_list[-1] += "\n" + "\n".join(title_buffer)
        else:
            merged_list.append("\n".join(title_buffer))

    return merged_list


def process_docx(file_path):
    record_id = os.path.splitext(os.path.basename(file_path))[0]

    text_list = extract_text(file_path)
    merged_text_list = merge_title_blocks(text_list)

    text_str = "\n----------\n".join(map(str, merged_text_list))

    with open("processed_docs/education_pickle/" + record_id + ".docx" + ".pkl", "wb") as f:
        pickle.dump(merged_text_list, f)

    with open("processed_docs/education_txt/" + record_id + ".docx" + ".txt", "w") as f:
        f.write(text_str)


directory = "docs/education"
docx_files = glob.glob(os.path.join(directory, "*.docx"))

with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
    executor.map(process_docx, docx_files)

print("Data inserted successfully")
