import os

from dotenv import load_dotenv
from PyPDF2 import PdfReader, PdfWriter

load_dotenv()


def extract_pages(filename):
    # 创建新的文件名
    output_filename = "pdfs/" + filename

    pdf_reader = PdfReader("test/" + filename)
    pdf_writer = PdfWriter()

    total_pages = len(pdf_reader.pages)

    # 提取第一页
    first_page = pdf_reader.pages[0]
    pdf_writer.add_page(first_page)

    # 提取最后两页
    for page_number in range(total_pages - 3, total_pages):
        page = pdf_reader.pages[page_number]
        pdf_writer.add_page(page)

    with open(output_filename, "wb") as output_pdf:
        pdf_writer.write(output_pdf)

    return output_filename
