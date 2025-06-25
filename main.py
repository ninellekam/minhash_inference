from flask import Flask, request, jsonify, g
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import os
import time
import logging
from logging.handlers import RotatingFileHandler
from utils import sliding_window_snippets, LMDBMinHashLSH, text2bow
from pathlib import Path
from elasticsearch import Elasticsearch
from datetime import datetime

log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir, 'app.log'),
            maxBytes=10000000,  # 10MB
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("minhash-api")

app = Flask(__name__)
CORS(app)
limiter = Limiter(app=app, key_func=get_remote_address, default_limits=["3000 per second"])

LSH_PATH = "/home/minhash_inference/data/minhash_index"
lsh = LMDBMinHashLSH(
    path=LSH_PATH,
    num_perm=128,
    num_buckets=16,
    num_bits=8,
    seed=228,
    readonly=True
)
es_index_name = 'actual_github_v1'
es = Elasticsearch("http://localhost:9200", verify_certs=False)

@app.route('/info', methods=['GET'])
def info():
    return jsonify({"status": "ok", "message": "MinHash API is running"}), 200

@app.before_request
def before_request():
    g.start_time = time.perf_counter()
    g.request_data = request.get_data(as_text=True)
    # Больше нет проверки авторизации!

@app.after_request
def after_request(response):
    elapsed_ms = (time.perf_counter() - g.start_time) * 1000
    logger.info(
        f"Request from {request.remote_addr} | "
        f"Path: {request.path} | "
        f"Body: {g.request_data} | "
        f"Status: {response.status_code} | "
        f"Response: {response.get_data(as_text=True)[:1000]} | "
        f"Time: {elapsed_ms:.2f}ms"
    )
    return response

@app.route('/api/search', methods=['POST'])
@limiter.limit("3000 per second")
def search():
    try:
        data = request.get_json(force=True)
        # Проверяем обязательные поля
        required = ['context', 'file_path', 'project_name', 'date']
        for key in required:
            if key not in data:
                logger.warning(f"Missing '{key}' in request")
                return jsonify({'error': f'Missing {key} in request body'}), 400

        context = data['context']
        file_path = data['file_path']
        project_name = data['project_name']
        date_str = data['date']
        k = data.get('ngram_size', 4)
        top_k = data.get('top_k', 100)
        ext = Path(file_path).suffix

        # Преобразуем дату
        try:
            date = datetime.fromisoformat(date_str).date()
        except Exception as e:
            logger.warning(f"Invalid date format: {date_str}")
            return jsonify({'error': 'Invalid date format (expected YYYY-MM-DD)'}), 400

        answers = []
        query_str = sliding_window_snippets(context, ext)
        query_bow = text2bow(query_str, k)
        results = lsh.search(query_bow, top_k=top_k)
        if not results:
            return jsonify({
                'results': [],
                'status': 'empty',
                'execution_time_ms': (time.perf_counter() - g.start_time) * 1000,
                'count': 0
            })
        ids = [r['id'] for r in results]
        response = es.mget(index=es_index_name, body={"ids": ids})
        for doc in response['docs']:
            src = doc.get('_source', {})
            if src and 'project_created' in src and 'project_full_name' in src:
                try:
                    doc_date = datetime.fromisoformat(src.get('project_created')).date()
                except Exception:
                    continue
                if doc_date < date and src.get('project_full_name') != project_name:
                    file_abspath = src.get('file_abspath')
                    snippet_text = src.get('snippet_text')
                    postfix_text = src.get('postfix_text') if src.get('postfix_text') else ''
                    answers.append(f'#{file_abspath}\n{snippet_text}\n{postfix_text}\n')

        elapsed = (time.perf_counter() - g.start_time) * 1000
        response = {
            'results': answers,
            'status': 'success',
            'execution_time_ms': elapsed,
            'count': len(answers)
        }
        return jsonify(response)
    except Exception as e:
        logger.error(f"Search error: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# Точка входа для запуска через gunicorn
app_prod = app

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

