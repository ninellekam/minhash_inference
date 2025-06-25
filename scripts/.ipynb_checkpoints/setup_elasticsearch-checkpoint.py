#!/usr/bin/env python3
import os
import time
import logging
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_elasticsearch(es, timeout=300):
    """Ждем пока Elasticsearch будет готов"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            if es.ping():
                logger.info("✅ Elasticsearch is ready!")
                return True
        except Exception as e:
            logger.info(f"⏳ Waiting for Elasticsearch... ({e})")
            time.sleep(5)
    return False

def setup_elasticsearch():
    es_host = 'localhost'
    es_port = os.getenv('ES_PORT', '9200')
    index_name = os.getenv('ES_INDEX_NAME', 'actual_github_v1')
    
    es = Elasticsearch(f"http://{es_host}:{es_port}", verify_certs=False)
    
    if not wait_for_elasticsearch(es):
        logger.error("❌ Elasticsearch is not ready!")
        return False
    
    # Проверяем существование индекса
    if es.indices.exists(index=index_name):
        logger.info(f"✅ Index '{index_name}' already exists")
        return True
    
    # Создаем индекс с настройками
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "project_full_name": {"type": "keyword"},
                "project_created": {"type": "date"},
                "file_abspath": {"type": "text"},
                "snippet_text": {"type": "text"},
                "postfix_text": {"type": "text"}
            }
        }
    }
    
    logger.info(f"🔧 Creating index '{index_name}'...")
    es.indices.create(index=index_name, body=index_settings)
    logger.info(f"✅ Index '{index_name}' created successfully!")
    
    return True

if __name__ == '__main__':
    setup_elasticsearch()
