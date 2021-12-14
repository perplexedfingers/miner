import logging
import time
from queue import Queue
from threading import RLock
from urllib.parse import urljoin

import requests

VERSION = '0.3.2'
SHARES_LOCK = RLock()
SHARE_REPORT_QUEUE = Queue()
HEADERS = {'user-agent': 'ton-pool-miner/' + VERSION}
DEFAULT_WALLET = 'EQBoG6BHwfFPTEUsxXW8y0TyHN9_5Z1_VIb2uctCd-NDmCbx'


def report_share():
    global SHARES_COUNT, SHARES_ACCEPTED, POOL_HAS_RESULTS
    n_tries = 5
    while True:
        input, giver, hash, tm, (pool_url, wallet) = SHARE_REPORT_QUEUE.get(True)
        is_devfee = wallet == DEFAULT_WALLET
        logging.debug('trying to submit share %s%s [input = %s, giver = %s, job_time = %.2f]' %
                      (hash.hex(), ' (devfee)' if is_devfee else '', input, giver, tm))
        for i in range(n_tries + 1):
            try:
                r = requests.post(
                    urljoin(pool_url, '/submit'),
                    json={'inputs': [input], 'giver': giver, 'miner_addr': wallet},
                    headers=HEADERS, timeout=4 * (i + 1)
                )
                d = r.json()
            except Exception as e:
                if i == n_tries:
                    if not is_devfee:
                        logging.warning('failed to submit share %s: %s' % (hash.hex(), e))
                    break
                if not is_devfee:
                    logging.warning('failed to submit share %s, retrying (%d/%d): %s' % (hash.hex(), i + 1, n_tries, e))
                time.sleep(0.5)
                continue
            if is_devfee:
                pass
            elif 'accepted' not in d:
                logging.info('found share %s' % hash.hex())
                with SHARES_LOCK:
                    SHARES_ACCEPTED += 1
            elif r.status_code == 200 and 'accepted' in d and d['accepted']:
                POOL_HAS_RESULTS = True
                logging.info('successfully submitted share %s' % hash.hex())
                with SHARES_LOCK:
                    SHARES_ACCEPTED += 1
            else:
                POOL_HAS_RESULTS = True
                logging.warning('share %s rejected (job was got %ds ago)' % (hash.hex(), int(time.time() - tm)))
            break
        if not is_devfee:
            with SHARES_LOCK:
                SHARES_COUNT += 1
