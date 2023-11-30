import json

from docx import Document


def extract_text(doc):
    # 初始化当前标题和内容
    current_heading = None
    content = []

    # 存储标题和内容的字典
    headings_with_content = {}

    for paragraph in doc.paragraphs:
        if paragraph.style.name == "Heading 2":
            # 如果当前已有标题和内容，存储它们
            if current_heading:
                headings_with_content[current_heading] = "\n".join(content)

            # 更新当前标题和重置内容
            current_heading = paragraph.text
            content = []
        else:
            # 如果不是标题，添加到内容中
            content.append(paragraph.text)

    # 确保最后一个标题的内容也被添加
    if current_heading:
        headings_with_content[current_heading] = "\n".join(content)

    return headings_with_content


file_name = "MFA/book2-1-3.docx"
# 打开Word文档
doc = Document(file_name)

# 提取标题和内容
headings_content = extract_text(doc)


system_content = "You are a world class expert in water treatment."

jsonl_data = []

for user_content, assistant_content in headings_content.items():
    # 创建单个JSON对象
    json_object = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
            {"role": "assistant", "content": assistant_content},
        ]
    }
    # 添加到列表
    jsonl_data.append(json.dumps(json_object))

# 将每个JSON对象转换为单行
jsonl_formatted_data = "\n".join(jsonl_data)

decoded_jsonl_data = jsonl_formatted_data.encode().decode("unicode_escape")

# 输出结果
print(decoded_jsonl_data)

with open("book.jsonl", "w") as f:
    f.write(jsonl_formatted_data)
