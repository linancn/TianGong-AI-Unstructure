import os

import boto3
from dotenv import load_dotenv
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

load_dotenv()


region = "us-east-1"
service = "aoss"
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token,
)


opensearch_client = OpenSearch(
    hosts=[{"host": os.environ.get("OPENSEARCH_ESG_URL"), "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=300,
)

# Create the index
index_name = "esg"
index_body = {
    "settings": {"analysis": {"analyzer": {"smartcn": {"type": "smartcn"}}}},
    "mappings": {
        "properties": {
            "pageNumber": {
                "type": "integer",
            },
            "text": {"type": "text", "analyzer": "smartcn"},
            "reportId": {
                "type": "keyword",
            },
        },
    },
}

opensearch_client.indices.create(index=index_name, body=index_body)
