import concurrent.futures
import os
import time

from tools.chunk_by_sci_pdf import sci_chunk

directory = "docs/journals/10.1002"

pdf_names = [os.path.join(directory, name) for name in os.listdir(directory)]

pdf_names = pdf_names[0:100]
# for pdf_name in pdf_names:
#     sci_chunk(pdf_name)

sci_chunk("docs/journals/10.1002/aenm.201190011.pdf")

# pdf_names = pdf_names[0:100]

start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(16) as executor:
    executor.map(sci_chunk, pdf_names)

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")
