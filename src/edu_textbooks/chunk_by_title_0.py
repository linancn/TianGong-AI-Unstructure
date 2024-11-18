import logging
import os
import pickle

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf


load_dotenv()

logging.basicConfig(
    filename="textbook_pickle_2.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)


# 读取pickle文件
# with open("list_0.pkl", "rb") as f:
#     records = pickle.load(f)


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


# record = "ca6d2bb1-1eea-4f88-9eb8-0a4e2294e559"

# process_pdf(record)

records = [
    "b42bd150-2c4e-41eb-982d-a69e4f1dda7a",
    "66c41088-dde4-432b-9f89-2fc989393922",
    "65f2ae9e-6d8d-49e3-bde5-d2ca8672c44a",
    "f5b5ee59-c902-458c-8fb8-4576eb41af7d",
    "0a039f4d-7a3a-4ff5-9366-3662687dfecb",
    "79290757-4886-4861-9eb1-cda9dd37fd2a",
    "6bc7dae7-6bf8-4af2-a9b6-ed61ec199e9c",
    "1ddee0ea-693d-472d-879b-1cd7b987c264",
    "53cd14df-249f-4532-a500-1754044f9db8",
    "ff4c515c-193d-48ea-ba15-97eac11305b3",
    "7b8c388d-1da7-4cb4-8c3d-c8c70d496dfd",
    "81021605-4d47-40f3-9dad-65e6b8287157",
    "61999122-be94-4030-9bd0-4db784036040"
]

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
