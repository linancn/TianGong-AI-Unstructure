import concurrent.futures
import os
import time

from tools.chunk_by_sci_pdf import sci_chunk

directory = "MFA"

pdf_names = [os.path.join(directory, name) for name in os.listdir(directory)]

# for pdf_name in pdf_names:
#     sci_chunk(pdf_name)
start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(max_workers=12) as executor:
    executor.map(sci_chunk, pdf_names)

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")
