import base64
import os

import pandas as pd
from dotenv import load_dotenv

from xata.client import XataClient

load_dotenv()

xata_api_key = os.getenv("XATA_API_KEY")
xata_db_url = os.getenv("XATA_DB_URL")
xata = XataClient(api_key=xata_api_key, db_url=xata_db_url)

# 读取数据
df = pd.read_excel("OEM_Report.xlsx", sheet_name="Sheet3")


def check_file(pdf_path):
    if not os.path.exists(pdf_path):
        print(f"文件不存在：{pdf_path}")
        return False
    if os.path.getsize(pdf_path) >= 30 * 1024 * 1024:
        print(f"文件过大（>=30MB）：{pdf_path}")
        return False
    return True


def insert_record(
    companyName,
    companyShortName,
    publicationDate,
    reportStartTime,
    reportEndTime,
    url,
    title,
    country,
    category,
    remark,
    include_pdf,
    report_field=None,
):
    try:
        record = {
            "companyName": companyName,
            "companyShortName": companyShortName,
            "publicationDate": publicationDate,
            "reportStartTime": reportStartTime,
            "reportEndTime": reportEndTime,
            "url": url,
            "title": title,
            "country": country,
            "category": category,
            "remark": remark,
        }
        if include_pdf:
            record["Report"] = report_field
        data = xata.records().insert("ESG_Reports", record)
        print(f"记录上传成功（{'包含' if include_pdf else '不包含'}PDF）：{title}")
    except Exception as e:
        print(f"上传记录{title}时发生错误:", e)
        if include_pdf:
            print(f"由于PDF问题，尝试重新上传不包含PDF的记录：{title}")
            insert_record(
                companyName,
                companyShortName,
                publicationDate,
                reportStartTime,
                reportEndTime,
                url,
                title,
                country,
                category,
                remark,
                False,
            )


# 遍历每条记录
for index, row in df.iterrows():
    companyName = row["companyName"]
    companyShortName = row["companyShortName"]
    publicationDate = row["publicationDate"]
    reportStartTime = row["reportStartTime"]
    reportEndTime = row["reportEndTime"]
    url = row["url"]
    title = row["title"]
    country = row["country"]
    category = row["category"]
    name = row["name"]
    remark = row["remark"]
    pdf_path = row["pdf_path"]

    include_pdf = check_file(pdf_path)
    report_field = None
    if include_pdf:
        with open(pdf_path, "rb") as pdf_file:
            base64_content = base64.b64encode(pdf_file.read()).decode("ascii")
        report_field = {
            "name": name,
            "mediaType": "application/octet-stream",
            "enablePublicUrl": False,
            "base64Content": base64_content,
            "signedUrlTimeout": 300,
        }

    insert_record(
        companyName,
        companyShortName,
        publicationDate,
        reportStartTime,
        reportEndTime,
        url,
        title,
        country,
        category,
        remark,
        include_pdf,
        report_field,
    )
