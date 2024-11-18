import logging
import os
import pickle

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf


load_dotenv()

logging.basicConfig(
    filename="textbook_pickle_3.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)


# 读取pickle文件
# with open("list_1.pkl", "rb") as f:
#     records = pickle.load(f)

records = [
    "53cd14df-249f-4532-a500-1754044f9db8",
    "ff4c515c-193d-48ea-ba15-97eac11305b3",
    "7b8c388d-1da7-4cb4-8c3d-c8c70d496dfd",
    "81021605-4d47-40f3-9dad-65e6b8287157",
    "61999122-be94-4030-9bd0-4db784036040"
]


def process_pdf(record_id):
    try:
        text_list = unstructure_pdf(pdf_name="temp/afterdec/" + record_id + ".pdf")

        with open("temp/pickles/" + record_id + ".pkl", "wb") as f:
            pickle.dump(text_list, f)

        text_str_list = [
            "Page {}: {}".format(page_number, text) for text, page_number in text_list
        ]
        text_str = "\n----------\n".join(text_str_list)
        with open("temp/txts/" + record_id + ".txt", "w") as f:
            f.write(text_str)
    except Exception as e:
        logging.error(f"Error processing {record_id}: {str(e)}")


# record = {"id": "af183ae1-c64b-417a-a19d-bf4d9611ce90", "language": "chi_sim"}

# safe_process_pdf(record)

for record in records:
    if os.path.exists("temp/pickles/" + record + ".pkl"):
        logging.info(f"Skipping {record}")
        continue
    else:
        logging.info(f"Processing {record}")
        process_pdf(record)
        logging.info(f"Processed {record}")


# with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
#     executor.map(safe_process_pdf, records)
