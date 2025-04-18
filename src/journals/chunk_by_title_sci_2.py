import logging
import os
from dotenv import load_dotenv
import pickle


# 创建自定义的日志记录器
logger = logging.getLogger('journal_processor')
logger.setLevel(logging.INFO)

# 创建文件处理器并设置格式
fh = logging.FileHandler('journal_pinecone_2.log', mode='w')
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
fh.setFormatter(formatter)

# 将处理器添加到日志记录器
logger.addHandler(fh)

from tools.chunk_by_sci_pdf import sci_chunk

load_dotenv()

# logging.basicConfig(
#     filename="journal_pinecone_2.log",
#     level=logging.INFO,
#     format="%(asctime)s:%(levelname)s:%(message)s",
#     filemode="w",
#     force=True,
# )

with open("journal_pdf_list_2.pkl", "rb") as f:
    pdf_lists = pickle.load(f)

# pdf_lists.reverse()

def safe_sci_chunk(pdf_list):
    try:
        return sci_chunk(pdf_list)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return None

for pdf_list in pdf_lists:
    pdf_path = pdf_list["pdf_path"]
    pdf_relative_path = pdf_path[len("docs/journals/") :]
    output_dir = "processed_docs/journal_pickle/"
    pickle_path = os.path.join(output_dir, f"{pdf_relative_path}.pkl")
    if not os.path.isfile(pickle_path):
        logger.info(f"Processing {pdf_list['doi']}")
        result = safe_sci_chunk(pdf_list)
        if result is None:
            logger.warning(f"Processing {pdf_list['doi']} failed")
        else:
            logger.info(f"Finished {pdf_list['doi']}")