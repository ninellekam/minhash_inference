#!/usr/bin/env python3
import requests
import time
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def health_check():
    api_key = os.getenv('API_KEY', '12aa')
    base_url = "http://localhost:8080"
    
    # Ждем пока сервер запустится
    for i in range(30):
        try:
            response = requests.get(f"{base_url}/", timeout=5)
            break
        except requests.exceptions.RequestException:
            logger.info(f"⏳ Waiting for server to start... ({i+1}/30)")
            time.sleep(2)
    else:
        logger.error("❌ Server did not start in time!")
        return False
    
    # Тестируем API
    test_data = {
        "context": "def test_function(): pass",
        "file_path": "test.py",
        "project_name": "test_project",
        "date": "2024-01-01"
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key
    }
    
    try:
        response = requests.post(
            f"{base_url}/api/search",
            json=test_data,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info("✅ API health check passed!")
            logger.info(f"Response: {response.json()}")
            return True
        else:
            logger.error(f"❌ API health check failed! Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ API health check failed! Error: {e}")
        return False

if __name__ == '__main__':
    health_check()
