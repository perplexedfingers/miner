from __future__ import annotations

import base64
import json
import logging
import os
import random
import ssl
import time
from threading import RLock
from urllib.parse import urljoin

import numpy as np
import requests

import sha256

VERSION = '0.3.2'
TASK_LOCK = RLock()
DEVFEE_POOL_URLS = ['https://next.ton-pool.club', 'https://next.ton-pool.com']
DEFAULT_WALLET = 'EQBoG6BHwfFPTEUsxXW8y0TyHN9_5Z1_VIb2uctCd-NDmCbx'
HASHES_COUNT_DEVFEE = 0
HASHES_COUNT = 0
HEADERS = {'user-agent': 'ton-pool-miner/' + VERSION}


def load_task(r, src, submit_conf):
    global CUR_TASK
    wallet_b64 = r['wallet']
    wallet = base64.urlsafe_b64decode(wallet_b64)
    assert wallet[1] * 4 % 256 == 0
    prefix = bytes(map(lambda x, y: x ^ y, b'\0' * 4 + os.urandom(28), bytes.fromhex(r['prefix']).ljust(32, b'\0')))
    input = b'\0\xf2Mine\0' + r['expire'].to_bytes(4, 'big') + wallet[2:34] + prefix + bytes.fromhex(r['seed']) + prefix
    complexity = bytes.fromhex(r['complexity'])

    hash_state = np.array(sha256.generate_hash(input[:64])).astype(np.uint32)
    suffix = bytes(input[64:]) + b'\x80'
    suffix_arr = []
    for j in range(0, 60, 4):
        suffix_arr.append(int.from_bytes(suffix[j:j + 4], 'big'))
    new_task = [0, input, r['giver'], complexity, hash_state, suffix_arr, time.time(), submit_conf, wallet_b64 == DEFAULT_WALLET]
    with TASK_LOCK:
        CUR_TASK = new_task
    logging.debug('successfully loaded new task from %s: %s' % (src, new_task))


def is_ton_pool_com(pool_url: str) -> bool:
    pool_url = pool_url.strip('/')
    return pool_url.endswith('.ton-pool.com') or pool_url.endswith('.ton-pool.club')


def hashrate_is_high() -> bool:
    return HASHES_COUNT_DEVFEE + 4 * 10**10 < HASHES_COUNT // 100


def need_to_collect_devfee(pool_url: str) -> bool:
    return is_ton_pool_com(pool_url) and hashrate_is_high()


def update_task_devfee(pool_url: str):
    while True:
        if need_to_collect_devfee(pool_url):
            try:
                url = random.choice(DEVFEE_POOL_URLS)
                r = requests.get(urljoin(url, '/job'), headers=HEADERS, timeout=10).json()
                load_task(r, 'devfee', (url, DEFAULT_WALLET))
            except Exception:
                pass
        time.sleep(5 + random.random() * 5)


def update_task(pool_url: str, wallet: str, limit: int):
    while True:
        try:
            r = requests.get(urljoin(pool_url, '/job'), headers=HEADERS, timeout=10).json()
            load_task(r, '/job', (pool_url, wallet))
        except Exception as e:
            logging.warning('failed to fetch new job: %s' % e)
            time.sleep(5)
            continue
        limit -= 1
        if limit == 0:
            return
        if WS_AVAILABLE:
            time.sleep(17 + random.random() * 5)
        else:
            time.sleep(3 + random.random() * 5)
        if time.time() - CUR_TASK[6] > 60:
            logging.error('failed to fetch new job for %.2fs, please check your network connection!' % (time.time() - CUR_TASK[6]))


def update_task_ws(pool_url: str, wallet: str):
    global WS_AVAILABLE
    try:
        from websocket import create_connection
    except Exception:
        logging.warning('websocket-client is not installed, will only use polling to fetch new jobs')
        return
    while True:
        try:
            r = requests.get(urljoin(pool_url, '/job-ws'), headers=HEADERS, timeout=10)
        except Exception:
            time.sleep(5)
            continue
        if r.status_code == 400:  # server asks to switch protocol
            break
        logging.warning('websocket job fetching is not supported by the pool, will only use polling to fetch new jobs')
        return
    WS_AVAILABLE = True

    ws_url = urljoin('ws' + pool_url[4:], '/job-ws')
    while True:
        try:
            ws = create_connection(ws_url, timeout=10, header=HEADERS, sslopt={'cert_reqs': ssl.CERT_NONE})
            while True:
                r = json.loads(ws.recv())
                load_task(r, '/job-ws', (pool_url, wallet))
        except Exception as e:
            logging.critical('=' * 50 + str(e))
            time.sleep(random.random() * 5 + 2)
