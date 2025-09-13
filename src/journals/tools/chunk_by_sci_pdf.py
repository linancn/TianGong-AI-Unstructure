import os
import pickle
import tempfile

import arrow
from dotenv import load_dotenv
from datetime import UTC, datetime
from openai import OpenAI
from pinecone import Pinecone
import psycopg2
from tools.vision import vision_completion
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
from unstructured.partition.pdf import partition_pdf


load_dotenv()

client = OpenAI()

pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY_US_EAST_1"))
idx = pc.Index(os.environ.get("PINECONE_SERVERLESS_INDEX_NAME_US_EAST_1"))


def get_doi(pdf_path):
    doi = pdf_path[14:-4]
    return doi


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


def fix_utf8(original_list):
    cleaned_list = []
    for original_str in original_list:
        cleaned_str = original_str.replace("\ufffd", " ")
        cleaned_list.append(cleaned_str)
    return cleaned_list


def get_embeddings(text_list, model="text-embedding-3-small"):
    try:
        text_list = [text.replace("\n\n", " ").replace("\n", " ") for text in text_list]
        length = len(text_list)
        results = []
        for i in range(0, length, 1000):
            results.append(
                client.embeddings.create(
                    input=text_list[i : i + 1000], model=model
                ).data
            )
        return sum(results, [])

    except Exception as e:
        print(e)


def check_misc(text):
    keywords_for_misc = [
        "ACKNOWLEDGEMENTS",
        "ACKNOWLEDGMENTS",
        "ACKNOWLEDGEMENT",
        "ACKNOWLEDGMENT",
        "BIBLIOGRAPHY",
        "DATAAVAILABILITY",
        "DECLARATIONOFCOMPETINGINTEREST",
        # "ONLINE",
        "REFERENCES",
        "SUPPLEMENTARYINFORMATION",
        "SUPPLEMENTARYMATERIALS",
        "SUPPORTINGINFORMATION",
        "参考文献",
        "致谢",
        "謝",
        "謝辞",
    ]

    text = text.strip()
    text = text.replace(" ", "")
    text = text.replace("\n", "")
    text = text.replace("\t", "")
    text = text.replace(":", "")
    text = text.replace("：", "")
    text = text.upper()

    if text in keywords_for_misc or any(
        keyword in text for keyword in keywords_for_misc
    ):
        return True


def extract_filename(path):
    base_name = os.path.basename(path)
    file_name, _ = os.path.splitext(base_name)
    return file_name


def sci_chunk(pdf_list, vision=False):
    pdf_path = pdf_list["pdf_path"]
    # 图像的最小尺寸要求
    min_image_width = 250
    min_image_height = 270

    # 分割文档
    try:
        elements = partition_pdf(
            filename=pdf_path,
            header_footer=False,
            pdf_extract_images=vision,
            pdf_image_output_dir_path=tempfile.gettempdir(),
            # pdf_infer_table_structure=True,
            skip_infer_table_types=["jpg", "png", "xls", "xlsx", "heic"],
            strategy="hi_res",
            hi_res_model_name="yolox",
            languages=["eng"],
        )
    except Exception as e:
        print(f"Error processing PDF {pdf_path}: {e}")
        return

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

    # 对文本和图像元素进行处理
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

    # 将文档分割成块
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

    if len(text_list) >= 2 and len(text_list[-1]) < 10:
        text_list[-2] = text_list[-2] + " " + text_list[-1]
        text_list = text_list[:-1]

    data = fix_utf8(text_list)

    pdf_relative_path = pdf_path[len("docs/journals/") :]
    output_dir = "processed_docs/journal_pickle/"
    pickle_path = os.path.join(output_dir, f"{pdf_relative_path}.pkl")

    embeddings = get_embeddings(data)

    doi = pdf_list["doi"]
    vectors = []
    for index, item in enumerate(data):
        vectors.append(
            {
                "id": doi + "_" + str(index),
                "values": embeddings[index].embedding,
                "metadata": {
                    "text": item,
                    "doi": doi,
                    "journal": pdf_list["journal"],
                    "date": to_unix_timestamp(pdf_list["date"]),
                },
            }
        )

    try:
        idx.upsert(
            vectors=vectors, batch_size=100, namespace="sci", show_progress=False
        )

        with open(pickle_path, "wb") as f:
            pickle.dump(data, f)

        conn_pg = psycopg2.connect(
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        try:
            with conn_pg.cursor() as cur:
                cur.execute(
                    "UPDATE journals SET embedding_time = %s WHERE doi = %s",
                    (datetime.now(UTC), doi),
                )
                conn_pg.commit()
                print(f"Updated embedding time: {doi}")
        except Exception as e:
            conn_pg.rollback()
            print(f"PostgreSQL error: {e}")
        finally:
            conn_pg.close()

    except Exception as e:
        print(f"Upsert error: {e}")

    print(f"Finished {pdf_path}")
