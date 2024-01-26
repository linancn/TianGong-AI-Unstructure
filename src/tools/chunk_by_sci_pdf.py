import os
import tempfile

from dotenv import load_dotenv
from openai import OpenAI
from pinecone import Pinecone
from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import clean, group_broken_paragraphs
from unstructured.documents.elements import (
    CompositeElement,
    Footer,
    Header,
    Image,
    Table,
    Title,
)
from unstructured.partition.auto import partition
from xata.client import XataClient

from tools.vision import vision_completion

load_dotenv()

client = OpenAI()

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_DOCS_DB_URL")
xata = XataClient(api_key=xata_api_key, db_url=xata_db_url)

pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY"))
idx = pc.Index(os.environ.get("PINECONE_SERVERLESS_INDEX_NAME"))


def get_doi(pdf_path):
    doi = pdf_path[14:-4]
    return doi


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


def sci_chunk(pdf_path, vision=False):
    # 图像的最小尺寸要求
    min_image_width = 250
    min_image_height = 270

    # 分割文档
    elements = partition(
        filename=pdf_path,
        header_footer=False,
        pdf_extract_images=vision,
        pdf_image_output_dir_path=tempfile.gettempdir(),
        skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
        strategy="auto",
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
        elif isinstance(chunk, Table):
            if text_list:
                text_list[-1] = text_list[-1] + "\n" + chunk.metadata.text_as_html
            else:
                text_list.append(chunk.hunk.metadata.text_as_html)

    if len(text_list) >= 2 and len(text_list[-1]) < 10:
        text_list[-2] = text_list[-2] + " " + text_list[-1]
        text_list = text_list[:-1]

    # with open(
    #     f"docs_output/{extract_filename(pdf_path)}.txt", "w", encoding="utf-8"
    # ) as f:
    #     for item in text_list:
    #         f.write("-----------------------------------\n")
    #         f.write("%s\n" % item)

    data = fix_utf8(text_list)
    embeddings = get_embeddings(data)

    doi = get_doi(pdf_path)
    vectors = []
    for index, item in enumerate(data):
        vectors.append(
            {
                "id": doi + "_" + str(index),
                "values": embeddings[index].embedding,
                "metadata": {"text": item},
            }
        )

    idx.upsert(vectors=vectors, batch_size=100, namespace="sci", show_progress=False)

    xata.sql().query(
        'UPDATE "journals" SET "embedding_time" = NOW() WHERE doi = $1', [doi]
    )

    print(f"Finished {pdf_path}")
