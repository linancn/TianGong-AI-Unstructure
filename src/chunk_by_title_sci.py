import concurrent.futures
import os
import time

from tools.chunk_by_sci_pdf import sci_chunk

directory = "docs/10.1002"

pdf_names = [os.path.join(directory, name) for name in os.listdir(directory)]

# for pdf_name in pdf_names:
#     sci_chunk(pdf_name)

# sci_chunk("MFA/2014-GEC-The role of in-use stocks in the social metabolism and in climate.pdf")

# pdf_names = pdf_names[0:100]

start_time = time.time()

with concurrent.futures.ProcessPoolExecutor(16) as executor:
    executor.map(sci_chunk, pdf_names)

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")
