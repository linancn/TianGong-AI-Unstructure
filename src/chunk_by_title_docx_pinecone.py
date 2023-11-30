import glob
import json
import os
import re
import uuid

import pinecone
import requests
from docx import Document
from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

client = OpenAI()

plugin_key = os.getenv("PLUGIN_KEY")
pinecone_api_key = os.getenv("PINECONE_API_KEY")
pinecone_environment = os.getenv("PINECONE_ENVIRONMENT")
pinecone_index = os.getenv("PINECONE_INDEX")

pinecone.init(api_key=pinecone_api_key, environment=pinecone_environment)
index = pinecone.Index(pinecone_index)


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


def openai_embedding(text_list: list[str], source: str):
    response = client.embeddings.create(
        input=text_list,
        model="text-embedding-ada-002",
    )
    vectors = []
    for text, embedding in zip(text_list, response.data):
        vector = {
            "id": str(uuid.uuid4()),
            "values": embedding.embedding,
            "metadata": {"text": text, "source": source},
        }
        vectors.append(vector)
    return vectors


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

    embedding_vectors = openai_embedding(
        text_list=contents, source=file_name_without_ext
    )

    upsert_response = index.upsert(
        vectors=embedding_vectors,
    )
