import os
import shutil
import logging
from urllib.parse import unquote

import pandas as pd

logging.basicConfig(
    filename="rename.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)


# 读取csv文件
df = pd.read_csv("journal_doi_id.csv", header=None, names=["id", "doi"])

df = df.astype({"id": "str", "doi": "str"})
df.set_index("doi", inplace=True)

base_dir = "processed_docs/journal_pickle/"
left_len = len(base_dir)
right_len = len(".pdf.pkl")

for root, _, files in os.walk(base_dir):
    for file in files:
        file_path = os.path.join(root, file)
        # 两次unquote解码
        decoded_path = unquote(unquote(file_path))
        doi = decoded_path[left_len:-right_len]

        new_name = df.loc[doi, "id"]
        try:
            new_path = os.path.join(base_dir, f"{new_name}.pkl")
            # 复制并重命名
            shutil.copy2(file_path, new_path)
            logging.info(f"{doi} renamed to {new_name}.pkl.")
        except Exception as e:
            logging.error(f"{doi} failed to rename: {e}")
