
import pickle

from tools.chunk_by_sci_pdf import sci_chunk


with open("journal_pdf_list_2.pkl", "rb") as f:
    pdf_lists = pickle.load(f)


def safe_sci_chunk(pdf_list):
    try:
        return sci_chunk(pdf_list)
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

for pdf_list in pdf_lists:
    safe_sci_chunk(pdf_list)