#!/usr/bin/env python3
import os
import boto3
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_s3_folder(s3_client, bucket, s3_folder, local_folder):
    """Скачивает папку с S3 рекурсивно"""
    local_path = Path(local_folder)
    local_path.mkdir(parents=True, exist_ok=True)
    
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_folder)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/'):  # Skip directories
                    continue
                
                # Создаем локальный путь
                relative_path = key.replace(s3_folder, '').lstrip('/')
                local_file_path = local_path / relative_path
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                logger.info(f"Downloading {key} -> {local_file_path}")
                s3_client.download_file(bucket, key, str(local_file_path))

def main():
    # Настройка S3 клиента
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    )
    
    bucket = os.getenv('S3_BUCKET')
    
    # Скачиваем MinHash индекс
    logger.info("📥 Downloading MinHash index...")
    download_s3_folder(
        s3_client,
        bucket,
        os.getenv('S3_MINHASH_PATH'),
        'data/minhash_index'
    )
    
    # Скачиваем данные Elasticsearch (если есть)
    if os.getenv('S3_ELASTICSEARCH_PATH'):
        logger.info("📥 Downloading Elasticsearch data...")
        download_s3_folder(
            s3_client,
            bucket,
            os.getenv('S3_ELASTICSEARCH_PATH'),
            'data/elasticsearch_data'
        )
    
    logger.info("✅ Download completed!")

if __name__ == '__main__':
    main()
