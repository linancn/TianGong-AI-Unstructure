import tempfile

import pandas as pd
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

from tools.vision import vision_completion

pdf_name = "raw/3.8-cutoff-20927-14.pdf"

min_image_width = 250
min_image_height = 270

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
    # elif isinstance(element, Image):
    #     point1 = element.metadata.coordinates.points[0]
    #     point2 = element.metadata.coordinates.points[2]
    #     width = abs(point2[0] - point1[0])
    #     height = abs(point2[1] - point1[1])
    #     if width >= min_image_width and height >= min_image_height:
    #         element.text = vision_completion(element.metadata.image_path)

chunks = chunk_by_title(
    elements=filtered_elements,
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
            text_list[-1] = text_list[-1] + "\n\n" + chunk.metadata.text_as_html
        else:
            text_list.append(chunk.metadata.text_as_html)
result_list = []

for text in text_list:
    split_text = text.split("\n\n", 1)
    if len(split_text) == 2:
        title, body = split_text
        result_list.append({"title": title, "body": body})

df = pd.DataFrame(result_list)
print(df)
df.to_excel("output.xlsx", index=True, header=True)


# for result in result_list:
#     print(result)
#     print("\n\n" + "-" * 80)
#     input()
