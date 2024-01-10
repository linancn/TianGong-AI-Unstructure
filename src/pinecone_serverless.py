import os

import numpy as np
from pinecone import Pinecone

pc = Pinecone(api_key=os.environ.get("PINECONE_SERVERLESS_API_KEY"))

idx = pc.Index("tiangong")


# print(idx.describe_index_stats())

array_1536 = np.random.rand(1536).tolist()

idx.upsert(
    [
        ("B", array_1536),
    ]
)
