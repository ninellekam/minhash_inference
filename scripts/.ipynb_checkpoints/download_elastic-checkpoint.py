import os
import boto3
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_gb(size_bytes):
    return size_bytes / (1024 ** 3)

def main():
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    )

    bucket = os.getenv('S3_BUCKET')
    s3_key = os.getenv('S3_ELASTICSEARCH_PATH')
    local_file = 'elasticsearch-8.18.0.tar.gz'

    # Получаем размер файла на S3
    response = s3_client.head_object(Bucket=bucket, Key=s3_key)
    total_bytes = response['ContentLength']

    progress = {'downloaded': 0}

    def progress_callback(bytes_amount):
        progress['downloaded'] += bytes_amount
        gb_left = format_gb(total_bytes - progress['downloaded'])
        print(f"\rОсталось скачать: {gb_left:.2f} ГБ", end="", flush=True)

    # Проверка: если локальный файл уже есть — удалить
    local_path = Path(local_file)
    if local_path.exists():
        logger.warning(f"Файл {local_path} уже существует, удаляю его перед скачиванием.")
        local_path.unlink()

    logger.info(f"📥 Скачиваю {s3_key} из бакета {bucket} в файл {local_file} ...")
    s3_client.download_file(bucket, s3_key, local_file, Callback=progress_callback)
    print("\n✅ Скачивание завершено!")

if __name__ == '__main__':
    main()