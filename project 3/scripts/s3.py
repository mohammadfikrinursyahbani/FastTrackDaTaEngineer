import s3fs
from dotenv import load_dotenv
import os
load_dotenv()

def s3_api():
    client = s3fs.S3FileSystem(
        key=os.getenv('MINIO_ACCESS_KEY'),
        secret=os.getenv('MINIO_SECRET_KEY'),
        client_kwargs={
            'endpoint_url': os.getenv('MINIO_URL')
        }
    )
    return client