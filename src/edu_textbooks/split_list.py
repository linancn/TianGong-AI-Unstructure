import os
import pickle
import math


def get_pdf_filenames(directory):
    """获取指定目录下的所有PDF文件名，并去掉后缀"""
    return [os.path.splitext(f)[0] for f in os.listdir(directory) if f.endswith(".pdf")]


def split_list(lst):
    """将列表平均分成两份"""
    mid = math.ceil(len(lst) / 2)
    return lst[:mid], lst[mid:]


def save_to_pickle(data, filename):
    """将数据保存到pickle文件"""
    with open(filename, "wb") as f:
        pickle.dump(data, f)


# 读取pdf名称列表
directory = "temp/afterdec"  # 替换为你的目录路径
pdf_filenames = get_pdf_filenames(directory)
list1, list2 = split_list(pdf_filenames)

save_to_pickle(list1, "list_0.pkl")
save_to_pickle(list2, "list_1.pkl")
