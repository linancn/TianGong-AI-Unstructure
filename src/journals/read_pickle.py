import pickle
import os

# from tools.chunk_by_sci_pdf import sci_chunk

with open("docs_s3.pkl", "rb") as f: 
    docs_s3 = pickle.load(f)

#读取processed_docs/journal_pickle/下所有的pickle文件的完整路径
def get_pickle_file_paths(directory):
    pickle_files = []
    # Walk through directory and subdirectories
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.pkl'):
                pickle_files.append(os.path.join(root, file))
    return pickle_files

# Example usage
directory = 'processed_docs/journal_pickle/'
docs_local = get_pickle_file_paths(directory)

# Convert lists to sets for set operations
docs_s3_set = set(docs_s3)
docs_local_set = set(docs_local)

# Find records in docs_s3 but not in docs_local
in_s3_not_in_local = docs_s3_set - docs_local_set

# Find records in docs_local but not in docs_s3
in_local_not_in_s3 = docs_local_set - docs_s3_set

#打印in_local_not_in_s3前100个
print(list(in_local_not_in_s3)[:100])

print(f"Number of records in s3 but not in local: {len(in_s3_not_in_local)}")


#merge pdf_lists
# pdf_lists = pdf_lists0 + pdf_lists1 + pdf_lists2 + pdf_lists3

# print(f"pdf_lists0: {len(pdf_lists0)}")
