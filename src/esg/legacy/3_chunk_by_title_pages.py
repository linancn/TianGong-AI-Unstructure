import concurrent.futures
import os
import pickle
import PyPDF2
from io import BytesIO

# import psycopg2
from dotenv import load_dotenv
from tools.unstructure_pdf_pages import unstructure_pdf

load_dotenv()


# conn_pg = psycopg2.connect(
#     database=os.getenv("POSTGRES_DB"),
#     user=os.getenv("POSTGRES_USER"),
#     password=os.getenv("POSTGRES_PASSWORD"),
#     host=os.getenv("POSTGRES_HOST"),
#     port=os.getenv("POSTGRES_PORT"),
# )

# with conn_pg.cursor() as cur:
#     cur.execute("SELECT id, language FROM esg_meta WHERE embedded_time IS NULL")
#     records = cur.fetchall()


files = os.listdir("esg_temp")

id = [file[:-4] for file in files]

# records = [record for record in records if record[0] not in id]

# ids = [record["id"] for record in records]
# print(ids)


def split_pdf_first_three_pages(input_pdf_path):
    pdf_reader = PyPDF2.PdfReader(input_pdf_path)
    pdf_writer = PyPDF2.PdfWriter()
    for page_num in range(min(3, len(pdf_reader.pages))):
        pdf_writer.add_page(pdf_reader.pages[page_num])
    output_pdf_io = BytesIO()
    pdf_writer.write(output_pdf_io)
    output_pdf_io.seek(0)
    return output_pdf_io


def process_pdf(file_id):
    # record_id = record[0]
    # if record[1] == "eng":
    #     language = ["eng"]
    # else:
    #     language = [record[1], "eng"]
    pdf_io = split_pdf_first_three_pages("esg_temp/" + file_id + ".pdf")

    text_list = unstructure_pdf(pdf_io=pdf_io)

    with open("esg_meta_pickle/" + file_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str_list = [
        "Page {}: {}".format(page_number, text) for text, page_number in text_list
    ]

    text_str = "\n----------\n".join(text_str_list)

    with open("esg_meta_txt/" + file_id + ".txt", "w") as f:
        f.write(text_str)


def safe_process_pdf(record):
    try:
        return process_pdf(record)
    except Exception as e:
        print(f"Error processing {record}: {str(e)}")
        return None


# record = {"id": "af183ae1-c64b-417a-a19d-bf4d9611ce90", "language": "chi_sim"}

# safe_process_pdf(id[0])

# for record in records:
#     process_pdf(record)


with concurrent.futures.ProcessPoolExecutor(max_workers=28) as executor:
    executor.map(safe_process_pdf, id)
