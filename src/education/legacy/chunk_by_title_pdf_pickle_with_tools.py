import concurrent.futures
import os
import pickle
import glob

from dotenv import load_dotenv
from tools.unstructure_pdf import unstructure_pdf

load_dotenv()

def process_pdf(file_path):
    record_id = os.path.splitext(os.path.basename(file_path))[0]

    text_list = unstructure_pdf(file_path)

    with open("pickle/" + record_id + ".pkl", "wb") as f:
        pickle.dump(text_list, f)

    text_str = "\n----------\n".join(text_list)

    with open("txt/" + record_id + ".txt", "w") as f:
        f.write(text_str)

directory = "test"
pdf_files = glob.glob(os.path.join(directory, "*.pdf"))

with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
    executor.map(process_pdf, pdf_files)
