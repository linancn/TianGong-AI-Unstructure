import pickle

# from tools.chunk_by_sci_pdf import sci_chunk

with open("docs_intersection_Oct26.pkl", "rb") as f: 
    pdf_paths_329367 = pickle.load(f)

with open("journal_pdf_list_0.pkl", "rb") as f:
    pdf_lists0 = pickle.load(f)

with open("journal_pdf_list_1.pkl", "rb") as f:
    pdf_lists1 = pickle.load(f)

with open("journal_pdf_list_2.pkl", "rb") as f:
    pdf_lists2 = pickle.load(f)

with open("journal_pdf_list_3.pkl", "rb") as f:
    pdf_lists3 = pickle.load(f)

#merge pdf_lists
pdf_lists = pdf_lists0 + pdf_lists1 + pdf_lists2 + pdf_lists3

print(f"pdf_lists0: {len(pdf_lists0)}")
