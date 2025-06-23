import os

class Config:
    # API
    API_KEY = os.getenv('API_KEY', '12aa')
    
    # Server
    HOST = os.getenv('SERVER_HOST', '0.0.0.0')
    PORT = int(os.getenv('SERVER_PORT', 8080))
    
    # Elasticsearch
    ES_HOST = os.getenv('ES_HOST', 'localhost')
    ES_PORT = int(os.getenv('ES_PORT', 9200))
    ES_INDEX_NAME = os.getenv('ES_INDEX_NAME', 'actual_github_v1')
    
    # MinHash
    MINHASH_PATH = os.getenv('MINHASH_PATH', '/app/data/minhash_index')
    MINHASH_NUM_PERM = int(os.getenv('MINHASH_NUM_PERM', 128))
    MINHASH_NUM_BUCKETS = int(os.getenv('MINHASH_NUM_BUCKETS', 16))
    MINHASH_NUM_BITS = int(os.getenv('MINHASH_NUM_BITS', 8))
    MINHASH_SEED = int(os.getenv('MINHASH_SEED', 228))
