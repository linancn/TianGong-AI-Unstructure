import os
import tempfile

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

from tools.vision import vision_completion


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

    with open(
        f"MFA_OUTPUT/{extract_filename(pdf_path)}.txt", "w", encoding="utf-8"
    ) as f:
        for item in text_list:
            f.write("-----------------------------------\n")
            f.write("%s\n" % item)

    # return text_list
