import json

from docx import Document
from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
from docx.oxml.ns import qn
from docx.shared import Pt
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI()


def get_formatted_text(message):
    response = client.chat.completions.create(
        model="gpt-4-1106-preview",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a world-class Chinese text editor, and your tasks include, but are not limited to: identifying typos in the text, removing all unnecessary spaces and blank lines, and correcting formatting errors. Answer in JSON format with key named 'result'.",
            },
            {"role": "user", "content": message},
        ],
    )
    return response.choices[0].message.content
