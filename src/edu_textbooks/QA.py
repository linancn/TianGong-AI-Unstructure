import json
import logging
import os
import pickle

import psycopg2
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

logging.basicConfig(
    filename="QA.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)


llm = ChatOpenAI(model="gpt-4o")

json_schema = {
    "title": "Question and Answer for LLM performance evaluation in the fieldf of environmental science",
    "description": "Generating questions and answers for evaluating LLM performance in the field of environmental science based on the input text",
    "type": "object",
    "properties": {
        "Question": {
            "type": "string",
            "description": "Question generated for evaluating LLM performance",
        },
        "Answer": {
            "type": "string",
            "description": "Answer to the question",
        },
        "Category": {
            "type": "string",
            "description": "Category of the question",
            "enum": ["环境工程学", "大气环境学", "水环境学"],
        },
    },
    "required": ["Question", "Answer", "Category"],
}
structured_llm = llm.with_structured_output(json_schema)

# 遍历文件夹中的pickle文件
for root, dirs, files in os.walk("temp/pickles"):
    for file in files:
        with open(os.path.join(root, file), "rb") as f:
            text_list = pickle.load(f)

        for i in range(0, len(text_list), 10):
            chunk = text_list[i : i + 10]
            if len(chunk) < 10:
                break

            # 将chunk转换为字符串
            chunk_text = " ".join([text[0] for text in chunk])

            # 执行structured_llm，生成3个问题和答案
            response = structured_llm.invoke(
                f"""Generate 3 challenging question-answer pairs to evaluate the capabilities of large language models and related applications (e.g., RAG systems) based on the following text: 
                {chunk_text}

                The questions should: Be complex and non-trivial, Test different aspects of language understanding and reasoning, Require synthesis of information, Have clear, verifiable answers. 
                The answers should: Be factually correct, Be supported by the text, Be unambiguous.
                Make the questions challenging enough that they would be difficult for basic LLMs but potentially solvable by more advanced systems with proper context and reasoning capabilities."""
            )
            print(response)
