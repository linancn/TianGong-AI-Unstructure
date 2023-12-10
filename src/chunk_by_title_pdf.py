import tempfile

from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import (
    clean,
    group_broken_paragraphs,
)
from unstructured.documents.elements import Footer, Header, Image, Text
from unstructured.partition.auto import partition

from tools.vision import vision_completion

pdf_name = "raw/BYD_CSR_2022.pdf"

elements = partition(
    filename=pdf_name,
    pdf_extract_images=True,
    pdf_image_output_dir_path=tempfile.gettempdir(),
    skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
    strategy="hi_res",
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
    elif isinstance(element, Image):
        element.text = vision_completion(element.metadata.image_path)


chunks = chunk_by_title(
    elements=filtered_elements,
    multipage_sections=True,
    combine_text_under_n_chars=0,
    new_after_n_chars=None,
    max_characters=4096,
)

for chunk in chunks:
    print(chunk)
    print("\n\n" + "-" * 80)
    input()
