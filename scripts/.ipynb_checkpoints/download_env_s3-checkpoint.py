import boto3
import os

bucket = os.getenv("S3_BUCKET", "kodas3")
s3_key = os.getenv("S3_ENV_KEY", "minhash_api_server/.env")
local_path = "/home/minhash_inference/.env"

aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
s3_endpoint = os.getenv("S3_ENDPOINT_URL", "https://storage.yandexcloud.net")

session = boto3.session.Session()
s3 = session.client(
    service_name='s3',
    endpoint_url=s3_endpoint,
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret,
)

print(f"🔒 Downloading {s3_key} from S3 to {local_path}")
s3.download_file(bucket, s3_key, local_path)
print("✅ .env downloaded from S3")
