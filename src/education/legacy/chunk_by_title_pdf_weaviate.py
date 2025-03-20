import glob
import re
import os
import tempfile
import weaviate
from weaviate.config import AdditionalConfig
from weaviate.classes.config import Configure, DataType, Property
import concurrent.futures


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
        "ACKNOWLEDGEMENTS",
        "ACKNOWLEDGMENTS",
        "ACKNOWLEDGEMENT",
        "ACKNOWLEDGMENT",
        "BIBLIOGRAPHY",
        "DATAAVAILABILITY",
        "DECLARATIONOFCOMPETINGINTEREST",
        "REFERENCES",
        "SUPPLEMENTARYINFORMATION",
        "SUPPLEMENTARYMATERIALS",
        "SUPPORTINGINFORMATION",
        "参考文献",
        "致谢",
        "謝",
        "謝辞",
        "AUTHOR INFORMATION",
        "ABBREVIATIONS",
        "ASSOCIATED CONTENT",
        "Read Online",
    ]

    text = text.strip()
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


def fix_utf8(original_list):
    cleaned_list = []
    for item in original_list:
        if isinstance(item, tuple):

            cleaned_tuple = tuple(s.replace("\ufffd", " ") for s in item)
            cleaned_list.append(cleaned_tuple)
        else:

            cleaned_str = item.replace("\ufffd", " ")
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
        languages=["chi", "eng"],
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
            element.text = group_broken_paragraphs(element.text)
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
            text = chunk.text
            text_list.append(text)
        elif isinstance(chunk, (Table, TableChunk)):
            text_as_html = getattr(chunk.metadata, "text_as_html", None)
            text_to_append = text_as_html if text_as_html is not None else chunk.text

            if text_list:
                text_list[-1] = text_list[-1] + "\n" + text_to_append
            else:
                text_list.append(text_to_append)

    result_list = []
    for text in text_list:
        split_text = text.split("\n\n", 1)
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


def process_pdf(file_path):
    file_name_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    contents = sci_chunk(file_path)
    w_chunks = split_chunks(text_list=contents, source=file_name_without_ext)
    water_collection = w_client.collections.get(name="Water_pdf")
    for chunk in w_chunks:
        water_collection.data.insert(chunk)


w_client = weaviate.connect_to_local(
    host="localhost", additional_config=AdditionalConfig(timeout=(600, 800))
)

try:
    collection = w_client.collections.create(
        name="Water_pdf",
        properties=[
            Property(name="title", data_type=DataType.TEXT),
            Property(name="content", data_type=DataType.TEXT),
            Property(name="source", data_type=DataType.TEXT),
        ],
        vectorizer_config=[
            Configure.NamedVectors.text2vec_transformers(
                name="title", source_properties=["title"]
            ),
            Configure.NamedVectors.text2vec_transformers(
                name="content", source_properties=["content"]
            ),
        ],
    )
    directory = "test"
    pdf_files = glob.glob(os.path.join(directory, "*.pdf"))
    with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
        executor.map(process_pdf, pdf_files)

    print("Data inserted successfully")

finally:
    w_client.close()
