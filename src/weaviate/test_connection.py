import weaviate, os
from weaviate.classes.init import Auth

# Best practice: store your credentials in environment variables
http_host = os.environ["WEAVIATE_HTTP_HOST"]
http_port = os.environ["WEAVIATE_HTTP_PORT"]
grpc_host = os.environ["WEAVIATE_GRPC_HOST"]
grpc_port = os.environ["WEAVIATE_GRPC_PORT"]
weaviate_api_key = os.environ["WEAVIATE_API_KEY"]

client = weaviate.connect_to_custom(
    http_host=http_host,        # Hostname for the HTTP API connection
    http_port=http_port,        # Default is 80, WCD uses 443
    http_secure=False,          # Whether to use https (secure) for the HTTP API connection
    grpc_host=grpc_host,        # Hostname for the gRPC API connection
    grpc_port=grpc_port,        # Default is 50051, WCD uses 443
    grpc_secure=False,          # Whether to use a secure channel for the gRPC API connection
    auth_credentials=Auth.api_key(weaviate_api_key),  # API key for authentication
)

print(client.is_ready())

client.close()
