import pickle
import concurrent.futures

from tools.chunk_by_sci_pdf import sci_chunk

with open("journal_pdf_list_0.pkl", "rb") as f:
    pdf_lists = pickle.load(f)

pdf_lists.reverse()


def safe_sci_chunk(pdf_list):
    try:
        return sci_chunk(pdf_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

for pdf_list in pdf_lists:
    try:
        print(f"Processing {pdf_list['doi']}")
        safe_sci_chunk(pdf_list)
    except Exception as e:
        print(f"Error processing {pdf_list['doi']}: {e}")
        continue
