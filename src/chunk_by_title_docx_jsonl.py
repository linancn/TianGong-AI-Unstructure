import glob
import json
import os
import re

import requests
from docx import Document
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

plugin_key = os.getenv("PLUGIN_KEY")


@retry(wait=wait_fixed(3), stop=stop_after_attempt(30))
def text_upsert(
    text_input,
    # key_input,
    source_input,
    # source_id_input,
    # url_input,
    # created_at_input,
    # author_input,
):
    url = "https://waterplugin.tiangong.world/upsert"

    headers = {
        "Authorization": f"Bearer {plugin_key}",
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    data = {
        "documents": [
            {
                # "id": key_input,
                "text": text_input,
                "metadata": {
                    "source": source_input,
                    # "source_id": source_id_input,
                    # "url": url_input,
                    # "created_at": created_at_input,
                    # "author": author_input,
                },
            }
        ]
    }

    data_string = json.dumps(data)

    response = requests.request("POST", url, headers=headers, data=data_string)
    response.raise_for_status()

    return response


def extract_text(docx):
    # 初始化当前标题和内容
    current_heading = None
    content = []

    # 最终存储的列表
    combined_content = []

    for paragraph in docx.paragraphs:
        if paragraph.style.name == "Heading 2":
            # 如果当前已有标题，将其及其内容作为一个元素添加到列表中
            if current_heading:
                combined_content.append(current_heading + "\n" + "\n".join(content))

            # 更新当前标题和重置内容
            current_heading = paragraph.text
            content = []
        else:
            # 如果不是标题，添加到内容中
            content.append(paragraph.text)

    # 确保最后一个标题及其内容也被添加
    if current_heading:
        combined_content.append(current_heading + "\n" + "\n".join(content))

    return combined_content


directory = "water"

# 遍历目录中的所有 .docx 文件
for file_path in glob.glob(os.path.join(directory, "*.docx")):
    # 获取文件名（不含后缀）
    file_name = os.path.basename(file_path)
    file_name_without_ext = re.split(r"\.docx$", file_name)[0]

    # 打开Word文档
    doc = Document(file_path)
    # 提取标题和内容
    contents = extract_text(doc)

    for content in contents:
        text_upsert(
            text_input=content,
            source_input=file_name_without_ext,
        )
        print(content)
