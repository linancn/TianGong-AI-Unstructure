import logging
import boto3

logging.basicConfig(
    filename="sci_remove.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)


# 创建 S3 客户端
s3_client = boto3.client("s3")

# S3 桶名和目录
bucket_name = "tiangong"
prefix = "processed_docs/journal_pickle/"


# 列出所有对象
paginator = s3_client.get_paginator("list_objects_v2")
page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

file_count = 0
for page in page_iterator:
    if "Contents" in page:
        for obj in page["Contents"]:
            # print(obj['Key'])
            file_count += 1

logging.info(f"总共有 {file_count} 个文件")

# for page in page_iterator:
#     if "Contents" in page:
#         for obj in page["Contents"]:
#             key = obj["Key"]
#             relative_key = key[len(prefix) :]

#             # 检查是否是顶层目录下的文件
#             if "/" not in relative_key:
#                 # 这是顶层文件，保留
#                 # print(f"Keeping top-level file: {key}")
#                 continue

#             # 检查是否是目录键（零字节对象，且键以 '/' 结尾）
#             if key.endswith("/") and obj["Size"] == 0:
#                 s3_client.delete_object(Bucket=bucket_name, Key=key)
#                 logging.info(f"Deleted directory key: {key}")
#                 continue  # 跳过目录键

#             # 删除子目录中的文件
#             logging.info(f"Deleting: {relative_key}")
#             # s3_client.delete_object(Bucket=bucket_name, Key=key)

# logging.info("所有子目录及其文件已被删除，顶层文件已保留。")
