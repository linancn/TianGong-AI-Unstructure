import concurrent.futures
import os
import time

from tools.chunk_by_sci_pdf import sci_chunk

directory = "docs/journals"

pdf_names = []
for dirpath, dirnames, filenames in os.walk(directory):
    for filename in filenames:
        pdf_names.append(os.path.join(dirpath, filename))

# pdf_names = pdf_names[0:100]
jie_pdf_names = []
for pdf_name in pdf_names:
    if "jiec" in pdf_name:
        jie_pdf_names.append(pdf_name)

# sci_chunk("docs/journals/10.1007/s11356-022-21798-3.pdf")

# pdf_names = pdf_names[0:100]

start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(16) as executor:
    executor.map(sci_chunk, jie_pdf_names)

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")
