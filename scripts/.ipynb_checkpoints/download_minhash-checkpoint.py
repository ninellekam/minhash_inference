import os
import boto3
from pathlib import Path
import logging
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_gb(size_bytes):
    return size_bytes / (1024 ** 3)

def download_s3_folder(s3_client, bucket, s3_folder, local_folder):
    """Скачивает папку с S3 рекурсивно с глобальным прогрессом"""
    local_path = Path(local_folder)
    local_path.mkdir(parents=True, exist_ok=True)
    
    # Собираем список файлов и общий размер
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=s3_folder)
    all_files = []
    total_bytes = 0
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/'):  # Skip directories
                    continue
                all_files.append((key, obj['Size']))
                total_bytes += obj['Size']
    logger.info(f"Всего файлов: {len(all_files)}, общий размер: {format_gb(total_bytes):.2f} ГБ")

    # Для корректного прогресса используем общий счетчик и lock (важно при многопоточности)
    progress = {'downloaded': 0}
    lock = threading.Lock()
    def progress_callback(bytes_amount):
        with lock:
            progress['downloaded'] += bytes_amount
            gb_left = format_gb(total_bytes - progress['downloaded'])
            print(f"\rОсталось скачать: {gb_left:.2f} ГБ", end="", flush=True)

    for idx, (key, size) in enumerate(all_files, 1):
        relative_path = key.replace(s3_folder, '').lstrip('/')
        local_file_path = local_path / relative_path
        local_file_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"[{idx}/{len(all_files)}] Downloading {key} -> {local_file_path} ({size/1024/1024:.2f} MB)")
        s3_client.download_file(bucket, key, str(local_file_path), Callback=progress_callback)

    print("\nСкачивание всех файлов завершено!")

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
    
    logger.info("✅ Download completed!")

if __name__ == '__main__':
    main()
