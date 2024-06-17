import glob
import re
import os
import tempfile
import concurrent.futures
import tiktoken
import pandas as pd
import logging
import pickle
from io import StringIO
from bs4 import BeautifulSoup

# from openai import OpenAI
from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import clean, group_broken_paragraphs
from unstructured.documents.elements import (
    CompositeElement,
    Footer,
    Header,
    Image,
    Table,
    TableChunk,
    Title,
)
from unstructured.partition.auto import partition

from tools.vision import vision_completion
from dotenv import load_dotenv

load_dotenv()

# client = OpenAI()


def check_misc(text):
    keywords_for_misc = [
        # "ACKNOWLEDGEMENTS",
        # "ACKNOWLEDGMENTS",
        # "ACKNOWLEDGEMENT",
        # "ACKNOWLEDGMENT",
        # "BIBLIOGRAPHY",
        # "DATAAVAILABILITY",
        # "DECLARATIONOFCOMPETINGINTEREST",
        # "REFERENCES",
        # "SUPPLEMENTARYINFORMATION",
        # "SUPPLEMENTARYMATERIALS",
        # "SUPPORTINGINFORMATION",
        # "参考文献",
        # "致谢",
        # "謝",
        # "謝辞",
        # "AUTHOR INFORMATION",
        # "ABBREVIATIONS",
        # "ASSOCIATED CONTENT",
        # "Read Online",
    ]

    text = str(text).strip()
    text = text.replace(" ", "")
    text = text.replace("\n", "")
    text = text.replace("\t", "")
    text = text.replace(":", "")
    text = text.replace("：", "")
    text = text.upper()

    return any(keyword in text for keyword in keywords_for_misc)


def extract_filename(path):
    base_name = os.path.basename(path)
    file_name, _ = os.path.splitext(base_name)
    return file_name


def num_tokens_from_string(string: str) -> int:
    """Returns the number of tokens in a text string."""
    encoding = tiktoken.get_encoding("cl100k_base")
    num_tokens = len(encoding.encode(string))
    return num_tokens


def fix_utf8(original_list):
    cleaned_list = []
    for item in original_list:
        if isinstance(item, tuple):
            cleaned_tuple = tuple(str(s).replace("\ufffd", " ") for s in item)
            cleaned_list.append(cleaned_tuple)
        else:
            cleaned_str = str(item).replace("\ufffd", " ")
            cleaned_list.append(cleaned_str)
    return cleaned_list


def sci_chunk(pdf_path, vision=False):
    min_image_width = 250
    min_image_height = 270

    elements = partition(
        filename=pdf_path,
        header_footer=False,
        pdf_extract_images=False,
        # pdf_extract_images=vision,
        pdf_image_output_dir_path=tempfile.gettempdir(),
        skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
        strategy="hi_res",
        hi_res_model_name="yolox",
        languages=["chi_sim", "eng"],
    )

    skip = False
    filtered_elements = []
    for element in elements:
        if skip:
            continue
        if isinstance(element, Title) and check_misc(element.text):
            skip = True
            continue
        if not (isinstance(element, Header) or isinstance(element, Footer)):
            filtered_elements.append(element)

    text_list = []
    for element in filtered_elements:
        if element.text != "":
            element.text = group_broken_paragraphs(str(element.text))
            element.text = clean(
                element.text,
                bullets=False,
                extra_whitespace=True,
                dashes=False,
                trailing_punctuation=False,
            )
        if vision:
            if isinstance(element, Image):
                point1 = element.metadata.coordinates.points[0]
                point2 = element.metadata.coordinates.points[2]
                width = abs(point2[0] - point1[0])
                height = abs(point2[1] - point1[1])
                if width >= min_image_width and height >= min_image_height:
                    element.text = vision_completion(element.metadata.image_path)

    chunks = chunk_by_title(
        elements=filtered_elements,
        multipage_sections=True,
        combine_text_under_n_chars=100,
        new_after_n_chars=512,
        max_characters=4096,
    )

    text_list = []

    for chunk in chunks:
        if isinstance(chunk, CompositeElement):
            text = str(chunk.text)
            text_list.append(text)
        elif isinstance(chunk, (Table, TableChunk)):
            text_as_html = getattr(chunk.metadata, "text_as_html", None)
            text_to_append = (
                text_as_html if text_as_html is not None else str(chunk.text)
            )

            if text_list:
                text_list[-1] = text_list[-1] + "\n" + text_to_append
            else:
                text_list.append(text_to_append)

    result_list = []
    for text in text_list:
        split_text = str(text).split("\n\n", 1)
        if len(split_text) == 2 and split_text[0].strip():
            title, content = split_text
        else:
            title = "Default Title"  # Or some other default value
            content = text
        result_list.append((title, content))
    return fix_utf8(result_list)


def split_chunks(text_list: list, source: str):
    chunks = []
    for title, content in text_list:  # Change this line
        chunks.append({"title": title, "content": content, "source": source})
    return chunks


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
        if num_tokens_from_string(d) > 8100:
            tables = BeautifulSoup(d, "html.parser").find_all("table")
            for table in tables:
                table_content = str(table)
                if num_tokens_from_string(table_content) < 8100:
                    if table_content:  
                        result.append(table_content)
                else:
                    try:
                        sub_tables = split_dataframe_table(table_content)
                        for sub_table in sub_tables:
                            if sub_table:
                                soup = BeautifulSoup(sub_table, "html.parser")
                                result.append(str(soup))
                    except Exception as e:
                        logging.error(e)
            logging.error(e)
        elif num_tokens_from_string(d) < 15:
            temp += d + " "
        else:
            result.append(temp + d)
            temp = ""
    if temp:
        result.append(temp)

    return result


def process_pdf(file_path):
    record_id = os.path.splitext(os.path.basename(file_path))[0]

    text_list = sci_chunk(file_path)

    with open("education_pickle/" + record_id + ".pdf" + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(
        f"{title}\n{content}" for title, content in text_list
    )

    with open("education_txt/" + record_id + ".pdf" + ".txt", "w") as f:
        f.write(text_str)


directory = "docs/education"
pdf_files = glob.glob(os.path.join(directory, "*.pdf"))

with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
    executor.map(process_pdf, pdf_files)

print("Data inserted successfully")
