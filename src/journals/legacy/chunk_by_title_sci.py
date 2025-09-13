import concurrent.futures
import pickle

from tools.chunk_by_sci_pdf import sci_chunk


with open("journal_pdf_list_0.pkl", "rb") as f:
    pdf_list = pickle.load(f)

for pdf in pdf_list:
    sci_chunk(pdf)


def safe_sci_chunk(pdf_list):
    try:
        return sci_chunk(pdf_list)
    except Exception as e:
        print(f"Error processing {pdf}: {str(e)}")
        return None


# start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(30) as executor:
    executor.map(safe_sci_chunk, pdf_list)

# end_time = time.time()


# print(f"Execution time: {end_time - start_time} seconds")
