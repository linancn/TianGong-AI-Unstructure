import tempfile

from dotenv import load_dotenv
from tools.vision import vision_completion
from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import clean, group_broken_paragraphs
from unstructured.documents.elements import (
    CompositeElement,
    Footer,
    Header,
    Image,
    Table,
)
from unstructured.partition.auto import partition

load_dotenv()


def unstructure_pdf(
    pdf_io, languages=["chi_sim", "chi_tra", "eng"], extract_images=False
):
    min_image_width = 250
    min_image_height = 270

    elements = partition(
        file=pdf_io,
        pdf_extract_images=extract_images,
        pdf_image_output_dir_path=tempfile.gettempdir(),
        skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
        strategy="hi_res",
        hi_res_model_name="yolox",
        languages=languages,
    )

    filtered_elements = [
        element
        for element in elements
        if not (isinstance(element, Header) or isinstance(element, Footer))
    ]

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
        if extract_images:
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
            page_number = chunk.metadata.page_number
            text_list.append((text, page_number))
        elif isinstance(chunk, Table):
            if text_list:
                text_list[-1] = (
                    text_list[-1][0] + "\n\n" + chunk.metadata.text_as_html,
                    text_list[-1][1],
                )
            else:
                page_number = chunk.metadata.page_number
                text_list.append((chunk.metadata.text_as_html, page_number))

    return text_list
