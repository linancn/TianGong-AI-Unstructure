from unstructured.chunking.title import chunk_by_title
from unstructured.partition.auto import partition
from unstructured.partition.docx import partition_docx

file_name = "water/book2-1-4.docx"

elements = partition_docx(
    filename=file_name,
    infer_table_structure=True,
    include_page_breaks=False,
)


chunks = chunk_by_title(elements, max_characters=1000)

for chunk in chunks:
    print(chunk)
    print("\n\n" + "-" * 80)
    input()
