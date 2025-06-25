from elasticsearch import Elasticsearch
from pathlib import Path
import pandas as pd
import regex as re
from datetime import datetime
from tqdm.notebook import tqdm
import sys
from minhashlsh_2 import LMDBMinHashLSH, text2bow
import json
import struct
import hashlib
import pickle
import os
import json
from typing import Set, List, Dict, Optional, Union, Iterator, Tuple
from collections import defaultdict
from pathlib import Path
import lmdb

import numpy as np
from tqdm import tqdm



def save_json(data, filename):
    """
    Сохраняет данные в формате JSON в указанный файл.

    :param data: Объект Python, который необходимо сохранить (например, dict или list).
    :param filename: Имя файла, в который будут сохранены данные.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Данные успешно сохранены в {filename}.")
    except Exception as e:
        print(f"Ошибка при сохранении данных: {e}")


def remove_multiline_comments(code, ext):
    # Простейшая обработка для Python, Java, JS, C/C++
    if ext in {'.py'}:
        # Удаляем тройные кавычки
        code = re.sub(r'(""".*?""")|(\'\'\'.*?\'\'\')', '', code, flags=re.DOTALL)
    elif ext in {'.js', '.java', '.cpp', '.c', '.h', '.hpp'}:
        code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
    # Можно добавить обработку других языков
    return code

def sliding_window_snippets(content, ext, snippet_len=15):
    code_wo_comments = remove_multiline_comments(content, ext)
    code_lines = [line for line in code_wo_comments.splitlines() if line.strip()][-snippet_len:]

    return '\n'.join(code_lines)


def search_snips_minhashlsh(
    lsh,
    es_index_name: str,
    context: str,
    ext: str,
    project_name: str,
    date: datetime=datetime.today().date(),
    return_snips=1,
    
    ) -> str:

    prep_con = sliding_window_snippets(context, ext)
    bow = text2bow(prep_con, 4)
    results = lsh.search(bow, top_k=100, limit=1000)
    if not results:
        return ''
    # ids = [r['id'] for r in results]
    # response = es.mget(index=es_index_name, body={"ids": ids})
    
    for doc in results['docs']:
        if datetime.fromisoformat(doc.get('_source').get('project_created')).date() < date and doc.get('_source').get('project_full_name') != project_name:
            file_abspath = doc.get('_source').get('file_abspath')
            snippet_text = doc.get('_source').get('snippet_text')
            postfix_text = doc.get('_source').get('postfix_text') if doc.get('_source').get('postfix_text') else ''
            return f'#{file_abspath}\n{snippet_text}\n{postfix_text}\n'

    return ''


def sha1_hash32(x: bytes) -> int:
    """
    32-Р±РёС‚РЅР°СЏ С…СЌС€-С„СѓРЅРєС†РёСЏ. РІР·СЏР» РёР· datasketch/hashfunc.py
    """
    return struct.unpack('<I', hashlib.sha1(x).digest()[:4])[0]


class LMDBMinHashLSH:
    def __init__(self, path, num_perm=128, num_buckets=16, num_bits=8, seed=228, r=None, readonly=False):
        self.env = lmdb.open(
            path,  # path — это директория!
            map_size=2**44,
            subdir=True,
            readonly=readonly,
            max_dbs=2,
            lock=False
        )

        self.num_perm = num_perm
        self.num_buckets = num_buckets
        self.num_bits = num_bits
        self.seed = seed
        self.r = r or (num_perm // num_buckets)
        # Определи dtype по num_bits
        self.dtype = np.uint8 if num_bits <= 8 else np.uint16 if num_bits <= 16 else np.uint32
        # Генераторы для minhash
        rs = np.random.RandomState(seed)
        self.a = rs.randint(low=1, high=2 ** 32, size=(num_perm,), dtype=np.uint64)[None]
        self.b = rs.randint(low=0, high=2 ** 32, size=(num_perm,), dtype=np.uint64)[None]
        self.m = 2 ** 32
        self.p = 2 ** 61 - 1
        self.max_value = 2 ** num_bits - 1

    def sha1_hash32(self, x: bytes) -> int:
        import struct, hashlib
        return struct.unpack('<I', hashlib.sha1(x).digest()[:4])[0]

    def encode(self, bow: set) -> np.ndarray:
        if not bow:
            return np.zeros(self.num_perm, dtype=self.dtype)
        x = np.array([self.sha1_hash32(t.encode("utf-8")) for t in bow], dtype=np.uint64).reshape(-1, 1)
        x = ((x @ self.a + self.b) % self.p) % self.m
        x = x.astype(np.uint32)
        x = x.min(0)
        x = np.bitwise_and(x, self.max_value)
        return x.astype(self.dtype)
    
    def encode_batch(self, bows: list) -> np.ndarray:
        """
        РљРѕРґРёСЂРѕРІР°РЅРёРµ Р±Р°С‚С‡Р° РґРѕРєСѓРјРµРЅС‚РѕРІ
        
        Args:
            bows: РЎРїРёСЃРѕРє РјРЅРѕР¶РµСЃС‚РІ n-РіСЂР°РјРј
            
        Returns:
            np.ndarray: РњР°СЃСЃРёРІ РєРѕРґРѕРІ MinHash
        """
        batch_size = len(bows)
        result = np.zeros((batch_size, self.num_perm), dtype=self.dtype)
        
        for i, bow in enumerate(bows):
            if not bow:  # РџСЂРѕРїСѓСЃРєР°РµРј РїСѓСЃС‚С‹Рµ РјРЅРѕР¶РµСЃС‚РІР°
                continue
                
            x = np.array([self.sha1_hash32(t.encode("utf-8")) for t in bow], dtype=np.uint64).reshape(-1, 1)  # [n, 1]
            x = ((x @ self.a + self.b) % self.p) % self.m   # [n, num_perm]
            x = x.astype(np.uint32)
            x = x.min(0)  # [num_perm]
            x = np.bitwise_and(x, self.max_value)
            result[i] = x.astype(self.dtype)
            
        return result

    def add(self, uuid: str, code: np.ndarray):
        with self.env.begin(write=True) as txn:
            txn.put(b'k2c_' + uuid.encode(), code.tobytes())
            for i in range(self.num_buckets):
                bucket_hash = code[i*self.r:(i+1)*self.r].tobytes()
                bucket_key = b'ht%d_' % i + bucket_hash
                ids = txn.get(bucket_key)
                if ids:
                    lst = pickle.loads(ids)
                else:
                    lst = []
                lst.append(uuid)
                txn.put(bucket_key, pickle.dumps(lst, protocol=4))

    def add_batch(self, uuids, codes):
        with self.env.begin(write=True) as txn:
            for idx, uuid in enumerate(uuids):
                code = codes[idx]
                txn.put(b'k2c_' + uuid.encode(), code.tobytes())
                for i in range(self.num_buckets):
                    bucket_hash = code[i*self.r:(i+1)*self.r].tobytes()
                    bucket_key = b'ht%d_' % i + bucket_hash
                    ids = txn.get(bucket_key)
                    if ids:
                        lst = pickle.loads(ids)
                    else:
                        lst = []
                    lst.append(uuid)
                    txn.put(bucket_key, pickle.dumps(lst, protocol=4))

    def get_candidates(self, code: np.ndarray, limit: int = -1):
        candidates = set()
        with self.env.begin() as txn:
            for i in range(self.num_buckets):
                bucket_hash = code[i*self.r:(i+1)*self.r].tobytes()
                bucket_key = b'ht%d_' % i + bucket_hash
                ids = txn.get(bucket_key)
                if ids:
                    lst = pickle.loads(ids)
                    for uid in lst:
                        candidates.add(uid)
                        if 0 < limit <= len(candidates):
                            return list(candidates)
        return list(candidates)

    def get_code(self, uuid: str):
        with self.env.begin() as txn:
            code_bytes = txn.get(b'k2c_' + uuid.encode())
            if code_bytes is None:
                return None
            return np.frombuffer(code_bytes, dtype=self.dtype)

    def search(self, bow_or_code, top_k=10, limit=10000):
        # bow_or_code: set или np.ndarray
        if isinstance(bow_or_code, set):
            code = self.encode(bow_or_code)
        else:
            code = bow_or_code
        candidates = self.get_candidates(code, limit=limit)
        if not candidates:
            return []
        # Считаем схожесть
        codes = []
        ids = []
        for uid in candidates:
            c = self.get_code(uid)
            if c is not None:
                codes.append(c)
                ids.append(uid)
        if not codes:
            return []
        code_arr = np.stack(codes, axis=0)
        sim = (code_arr == code).mean(1)
        sorted_idx = np.argsort(-sim)
        result = []
        for i in sorted_idx[:top_k]:
            result.append({'id': ids[i], 'score': float(sim[i])})
        return result
    
    def get_index_size(self):
        """
        Получение информации о размере индекса

        Returns:
            dict: Информация о размере индекса
        """
        # Подсчет количества ключей
        num_items = 0
        with self.env.begin() as txn:
            cursor = txn.cursor()
            prefix = b'k2c_'
            if cursor.set_range(prefix):
                while cursor.key().startswith(prefix):
                    num_items += 1
                    if not cursor.next():
                        break

        # Информация о размере БД (корректно)
        mdb_path = os.path.join(self.env.path(), "data.mdb")
        try:
            total_size = os.path.getsize(mdb_path)
        except Exception:
            total_size = 0

        return {
            "num_items": num_items,
            "total_memory_mb": total_size / (1024 * 1024),
            "memory_usage_mb": total_size / (1024 * 1024),  # для совместимости
            "codes_memory_mb": 0  # для совместимости, не применимо к LMDB
        }



import re

def text2bow(text: str, k: int, max_ngrams: int = None) -> set:
    """
    Оптимизированная версия text2bow с сохранением нормализации пробелов.
    
    Args:
        text: Исходный текст
        k: Размер n-грамм
        max_ngrams: Максимальное количество n-грамм (None = без ограничения)
        
    Returns:
        Set[str]: Множество n-грамм
    """
    # Нормализация пробелов с помощью регулярного выражения
    # \s+ соответствует любой последовательности пробельных символов (пробелы, табуляции, переносы строк)
    s = re.sub(r'\s+', ' ', text).strip()
    
    # Оптимизация: предварительно рассчитываем длину
    length = len(s)
    
    # Если текст слишком короткий
    if length <= k:
        return {s} if s else set()
    
    # Определяем, сколько n-грамм нужно создать
    ngram_count = length - k + 1
    if max_ngrams and ngram_count > max_ngrams:
        # Если задано ограничение и текст слишком длинный,
        # равномерно выбираем n-граммы из всего текста
        step = ngram_count / max_ngrams
        indices = [int(i * step) for i in range(max_ngrams)]
        return {s[i:i+k] for i in indices}
    
    # Создаем множество n-грамм напрямую через генератор множеств
    # Это более эффективно, чем добавление по одной n-грамме в цикле
    
    return {s[i:i+k] for i in range(ngram_count)}



def process_documents_in_batches(documents_iterator: Iterator[Tuple[str, str]],
                                lsh: LMDBMinHashLSH,
                                k: int = 3,
                                batch_size: int = 10000) -> None:
    """
    РћР±СЂР°Р±РѕС‚РєР° РґРѕРєСѓРјРµРЅС‚РѕРІ РёР· РёС‚РµСЂР°С‚РѕСЂР° Р±Р°С‚С‡Р°РјРё

    Args:
        documents_iterator: РС‚РµСЂР°С‚РѕСЂ, РІРѕР·РІСЂР°С‰Р°СЋС‰РёР№ РїР°СЂС‹ (id, text)
        lsh: Р­РєР·РµРјРїР»СЏСЂ MinHashLSH
        k: Р Р°Р·РјРµСЂ n-РіСЂР°РјРј
        batch_size: Р Р°Р·РјРµСЂ Р±Р°С‚С‡Р°
    """
    batch_keys = []
    batch_bows = []

    for doc_id, text in documents_iterator:
        batch_keys.append(doc_id)
        batch_bows.append(text2bow(text, k))

        if len(batch_keys) >= batch_size:
            batch_codes = lsh.encode_batch(batch_bows)
            lsh.add_batch(batch_keys, batch_codes)
            batch_keys = []
            batch_bows = []

    # РћР±СЂР°Р±Р°С‚С‹РІР°РµРј РѕСЃС‚Р°РІС€РёРµСЃСЏ РґРѕРєСѓРјРµРЅС‚С‹
    if batch_keys:
        batch_codes = lsh.encode_batch(batch_bows)
        lsh.add_batch(batch_keys, batch_codes)