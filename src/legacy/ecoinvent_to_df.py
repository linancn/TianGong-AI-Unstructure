import json
import os

import pandas as pd


def replace_authors(row):
    if row["Additional Authors"] not in [None, "None", []]:
        return row["Additional Authors"]
    else:
        return row["Additional Author(s)"]


def load_json_to_df(directory):
    dfs = []
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            prefix = filename.split(".")[0]  # get the prefix of the filename
            with open(os.path.join(directory, filename), "r") as f:
                data = json.load(f)
                result = data["result"]
                if (
                    isinstance(result, dict) and result
                ):  # check if the dict is not empty
                    df = pd.DataFrame([result])
                elif (
                    isinstance(result, list) and result
                ):  # check if the list is not empty
                    df = pd.DataFrame(result)
                df["filename_prefix"] = prefix  # add the new column
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


directory = "output"  # replace with your directory
df = load_json_to_df(directory)
df["Additional Author(s)"] = df.apply(replace_authors, axis=1)

df[["Year", "Volume Number", "Issue Number"]] = (
    df[["Year", "Volume Number", "Issue Number"]].replace("", "0").fillna(0).astype(int)
)


df[["Year", "Volume Number", "Issue Number"]] = df[
    ["Year", "Volume Number", "Issue Number"]
].replace(0, None)

print(df)

df.to_excel("output.xlsx", index=False)
