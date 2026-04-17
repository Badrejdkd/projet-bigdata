import boto3
from botocore.exceptions import ClientError

MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin123",
}

BUCKETS = [
    "datalake",
]

PREFIXES = [
    "bronze/books/",
    "silver/books/",
    "gold/analytics/",
    "logs/",
    "quarantine/",
]

def init():
    client = boto3.client("s3", **MINIO_CONFIG)

    for bucket in BUCKETS:
        try:
            client.create_bucket(Bucket=bucket)
            print(f"[OK] Bucket créé : {bucket}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                print(f"[--] Bucket existant : {bucket}")
            else:
                raise

    # Créer les préfixes (dossiers virtuels)
    for prefix in PREFIXES:
        client.put_object(Bucket="datalake", Key=prefix, Body=b"")
        print(f"[OK] Préfixe créé : {prefix}")

if __name__ == "__main__":
    init()