import json
import os
import re
import tempfile

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

# from PyPDF2 import PdfReader, PdfWriter
from tenacity import retry, stop_after_attempt, wait_fixed
from unstructured.chunking.title import chunk_by_title
from unstructured.cleaners.core import clean, group_broken_paragraphs
from unstructured.documents.elements import (
    CompositeElement,
    Footer,
    Header,
    Image,
    NarrativeText,
    Table,
    Title,
)
from unstructured.partition.auto import partition

load_dotenv()
openai_client = OpenAI()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def create_completion(**kwargs):
    return openai_client.chat.completions.create(**kwargs)


# def extract_pages(filename):
#     # 获取文件名（不包括扩展名）
#     base_name = os.path.splitext(filename)[0]
#     # 创建新的文件名
#     output_filename = "pdfs/" + base_name + "_new.pdf"

#     pdf_reader = PdfReader("test/" + filename)
#     pdf_writer = PdfWriter()

#     total_pages = len(pdf_reader.pages)

#     # 提取第一页
#     first_page = pdf_reader.pages[0]
#     pdf_writer.add_page(first_page)

#     # 提取最后两页
#     for page_number in range(total_pages - 3, total_pages):
#         page = pdf_reader.pages[page_number]
#         pdf_writer.add_page(page)

#     with open(output_filename, "wb") as output_pdf:
#         pdf_writer.write(output_pdf)

#     return output_filename


directory = "datareport"

for pdf_name in os.listdir(directory):
    # new_pdf_name = extract_pages(pdf_name)
    file_path = os.path.join(directory, pdf_name)

    elements = partition(
        filename=file_path,
        pdf_extract_images=False,
        pdf_image_output_dir_path=tempfile.gettempdir(),
        skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
        # strategy="hi_res",
    )

    cleaned_elements = [
        element
        for element in elements
        if not (isinstance(element, Header) or isinstance(element, Footer))
    ]

    processname_element = [
        element.text
        for element in elements
        if "ecoinvent 3.8 Dataset Documentation" in element.text
    ]
    match = re.search(r"'(.*?)'", str(processname_element))
    if match:
        process_name = match.group(1)

    # Initialize two flag variables
    found_source_header = False
    found_restriction_text = False

    filtered_elements = []
    last_source_index = None
    last_restriction_text_index = None
    # Find the last "Source" index
    for i, element in enumerate(elements):
        if isinstance(element, Title) and element.text == "Source":
            last_source_index = i
        if element.text.startswith("Restriction of Use"):
            last_restriction_text_index = i

    for i, element in enumerate(elements):
        # If the element is a Header and its text is "source", set the first flag to True
        # Also check if this is the last "Source"
        if isinstance(element, Title) and element.text == "Source":
            if i == last_source_index:
                found_source_header = True
        # If the element is a NarrativeText and its text is the restriction text, set the second flag to True
        if element.text.startswith("Restriction of Use"):
            if i == last_restriction_text_index:
                found_restriction_text = True
        # If the first flag is True and the second flag is False, add the element to the filtered_elements list
        if found_source_header and not found_restriction_text:
            filtered_elements.append(element)

    # for element in filtered_elements:
    #     if element.text != "":
    #         element.text = group_broken_paragraphs(element.text)
    #         element.text = clean(
    #             element.text,
    #             bullets=False,
    #             extra_whitespace=True,
    #             dashes=False,
    #             trailing_punctuation=False,
    #         )

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
                text_list[-1] = (
                    text_list[-1]
                    + "\n\n"
                    + chunk.text
                    + "\n\n"
                    + chunk.metadata.text_as_html
                )
            else:
                text_list.append(chunk.text + "\n\n" + chunk.metadata.text_as_html)

    # result_list = []

    # for text in text_list:
    #     split_text = text.split("\n\n", 1)
    #     if len(split_text) == 2:
    #         title, body = split_text
    #         result_list.append({"title": title, "body": body})

    # msgs = [
    #     SystemMessage(
    #         content="Generate JSON based on the text below."
    #     ),
    #     HumanMessage(content="Text:"),
    #     HumanMessagePromptTemplate.from_template("{result_list}"),
    # ]

    response = create_completion(
        model="gpt-4-1106-preview",
        response_format={"type": "json_object"},
        temperature=0.0,
        messages=[
            {
                "role": "system",
                "content": "You are programmed as an efficient assistant, tasked with generating output in JSON format. Your output should include an array, designated by square brackets [], and the array should be labeled with the key 'result'.",
            },
            {
                "role": "user",
                "content": f"""Carefully discern and extract the following bibliographic information from the provided text and output it in json format. The details to be extracted are: First Author, Additional Author(s), Title, Year, Volume Number, Issue Number, and Journal Name. The information to be processed is as follows: \n\n{str(text_list)}""",
            },
        ],
    )

    result = response.choices[0].message.content

    dict_data = json.loads(result)
    # Split the CSV content into lines
    if "CSV_Content" in dict_data:
        lines = dict_data["CSV_Content"].split("\n")

        # Process each line
        for i in range(len(lines)):
            # Skip the header line
            if i == 0:
                continue
            # Add processname and filename to the beginning of the line
            lines[i] = f"{process_name},{pdf_name}," + lines[i]

        lines = lines[1:]
        # Join the lines back into a single string
        new_csv_content = "\n".join(lines)

    # Write the new CSV content to the file
    with open("test.csv", "a+") as f:
        f.write(new_csv_content)

    with open("test.csv", "a+") as f:
        f.write(dict_data["CSV_Content"])


# df = pd.DataFrame(result_list)
# print(df)
# df.to_excel("output.xlsx", index=True, header=True)


# for result in result_list:
#     print(result)
#     print("\n\n" + "-" * 80)
#     input()
