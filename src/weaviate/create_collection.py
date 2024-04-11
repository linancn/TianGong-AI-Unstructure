from dotenv import load_dotenv

import weaviate
from weaviate.classes.config import Configure, DataType, Property
from weaviate.config import AdditionalConfig

load_dotenv()


w_client = weaviate.connect_to_local(
    host="localhost", additional_config=AdditionalConfig(timeout=(600, 800))
)

try:
    collection = w_client.collections.create(
        name="tiangong",
        properties=[
            Property(name="content", data_type=DataType.TEXT),
            Property(name="source", data_type=DataType.TEXT),
        ],
        vectorizer_config=[
            Configure.NamedVectors.text2vec_transformers(
                name="content", source_properties=["content"]
            )
        ],
    )

    # w_client.collections.delete(name="water")

finally:
    w_client.close()
