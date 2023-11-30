from unstructured.chunking.title import chunk_by_title
from unstructured.partition.auto import partition

pdf_name = "MFA/book2-1-3.docx"

elements = partition(
    filename=pdf_name,
    pdf_extract_images=True,
    skip_infer_table_types=["jpg", "png", "xls", "xlsx"],
    strategy="hi_res",
    include_page_breaks=False,
)


chunks = chunk_by_title(elements, max_characters=1000)

for chunk in chunks:
    print(chunk)
    print("\n\n" + "-" * 80)
    input()
