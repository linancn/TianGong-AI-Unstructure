from unstructured.chunking.title import chunk_by_title
from unstructured.partition.auto import partition
from unstructured.documents.elements import Text, ElementMetadata
from unstructured.cleaners.core import (
    clean,
    group_broken_paragraphs,
    clean_ordered_bullets,
    remove_punctuation,
)
from dataclasses import dataclass

pdf_name = "raw/BYD_CSR_2022.pdf"

elements = partition(
    filename=pdf_name,
    pdf_extract_images=False,
    skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
    strategy="hi_res",
)

filtered_elements = [
    element
    for element in elements
    if not (isinstance(element, Text) and (element.category == "Header" or "Footer"))
]

for element in filtered_elements:
    if isinstance(element, Text):
        element.text = group_broken_paragraphs(element.text)
        element.text = clean(
            element.text,
            bullets=True,
            extra_whitespace=True,
            dashes=True,
            trailing_punctuation=False,
        )
        element.text = clean_ordered_bullets(element.text)
        # element.text = remove_punctuation(element.text)

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
