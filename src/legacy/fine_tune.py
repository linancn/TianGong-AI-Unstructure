from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI()

response = client.chat.completions.create(
    model="ft:gpt-3.5-turbo-1106:personal::8QWAI3rH",
    messages=[
        {"role": "user", "content": "污水处理厂通水和联动试车的目的和条件是什么"},
    ],
)
print(response.choices[0].message)
