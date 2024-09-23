import pickle

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf


load_dotenv()


#读取pickle文件
with open("chunk_0.pkl", "rb") as f:
    records = pickle.load(f)


def process_pdf(record):
    record_id = record[0]
    language = [record[1]]

    text_list = unstructure_pdf(
        pdf_name="docs/esg/" + record_id + ".pdf", languages=language
    )

    with open("processed_docs/esg_pickle/" + record_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str_list = [
        "Page {}: {}".format(page_number, text) for text, page_number in text_list
    ]

    text_str = "\n----------\n".join(text_str_list)

    with open("processed_docs/esg_txt/" + record_id + ".txt", "w") as f:
        f.write(text_str)


def safe_process_pdf(record):
    try:
        return process_pdf(record)
    except Exception as e:
        print(f"Error processing {record}: {str(e)}")
        return None


# record = {"id": "af183ae1-c64b-417a-a19d-bf4d9611ce90", "language": "chi_sim"}

# safe_process_pdf(record)

for record in records:
    safe_process_pdf(record)


# with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
#     executor.map(safe_process_pdf, records)
