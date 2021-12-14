# This file belongs to TON-Pool.com Miner (https://github.com/TON-Pool/miner)
# License: GPLv3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from threading import Thread
from urllib.parse import urljoin

import pyopencl as cl
import requests

from receiver import update_task, update_task_devfee, update_task_ws
from reporter import report_share
from worker import Worker, get_device_id

VERSION = '0.3.2'  # constant
HEADERS = {'user-agent': 'ton-pool-miner/' + VERSION}  # contant
HASHES_COUNT = 0  # from worker
SHARES_COUNT = 0  # from reporter
POOL_HAS_RESULTS = False  # from reporter
SHARES_ACCEPTED = 0  # from reporter


def print_info(version: str) -> None:
    print('TON-Pool.com Miner', version)
    platforms = cl.get_platforms()
    for i, platform in enumerate(platforms):
        print(f'Platform {i}:')
        for j, device in enumerate(platform.get_devices()):
            print(f'    Device {j}: {get_device_id(device)}')


def print_usage(version: str) -> None:
    print('TON-Pool.com Miner', version)
    print(f'Usage: {sys.argv[0]} [pool url] [wallet address]')
    print('Run "{sys.argv[0]} info" to check your system info')
    print('Run "{sys.argv[0]} -h" to for detailed arguments')


def set_parser(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument('-p', dest='PLATFORM', help='Platform ID')
    parser.add_argument('-d', dest='DEVICE', help='Device ID')
    parser.add_argument('-t', dest='THREADS', help='Number of threads. This is applied for all devices.')
    parser.add_argument('--stats', dest='STATS', action='store_true', help='Dump stats to stats.json')
    parser.add_argument('--debug', dest='DEBUG', action='store_true', help='Show all logs')
    parser.add_argument('--silent', dest='SILENT', action='store_true', help='Only show warnings and errors')
    parser.add_argument('POOL', help='Pool URL')
    parser.add_argument('WALLET', help='Your wallet address')
    return parser


def set_logger(args: argparse.Namespace) -> None:
    if args.DEBUG:
        log_level = 'DEBUG'
    elif args.SILENT:
        log_level = 'WARNING'
    else:
        log_level = 'INFO'
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=log_level)


def test_pool_connection(version: str, pool_url: str, wallet: str, headers: dict) -> None:
    logging.info('starting TON-Pool.com Miner %s on pool %s wallet %s ...' % (version, pool_url, wallet))
    r = requests.get(urljoin(pool_url, '/wallet/' + wallet), headers=headers, timeout=10)
    r = r.json()
    r['ok']
    update_task(1)


def start_receiver() -> None:
    Thread(target=update_task, args=(0,), daemon=True).start()
    Thread(target=update_task_devfee, daemon=True).start()
    Thread(target=update_task_ws, daemon=True).start()


def start_reporter() -> None:
    for _ in range(8):
        Thread(target=report_share, daemon=True).start()


class PlatformIDError(IndexError):
    pass


class DeviceIDError(IndexError):
    pass


def select_devices(specified_platform_id: None or str = None, specified_device_id: None or str = None) -> list:
    platforms = cl.get_platforms()
    if specified_platform_id:
        t = int(specified_platform_id)
        try:
            platforms = [platforms[t]]
        except IndexError:
            raise PlatformIDError

    if not specified_device_id:
        devices = [platform.get_devices() for platform in platforms]
    else:
        devices = []
        for platform in platforms:
            cur_devices = platform.get_devices()
            try:
                t = int(specified_device_id)
                cur_devices = [cur_devices[t]]
            except IndexError:
                raise DeviceIDError
            devices += cur_devices
    return devices


def load_opencl_script() -> str:
    path = os.path.dirname(os.path.abspath(__file__))
    prog = open(os.path.join(path, 'sha256.cl'), 'r').read()\
        + '\n'\
        + open(os.path.join(path, 'hash_solver.cl'), 'r').read()
    return prog


def start_worker(devices, thead_count_in_worker) -> None:
    prog = load_opencl_script()
    for i, device in enumerate(devices):
        w = Worker(device, prog, thead_count_in_worker, i)
        Thread(target=w.run, daemon=True).start()


def report_hash_rate(_ss: list, cnt: int) -> list:
    ss = _ss.copy()
    ss.append((time.time(), HASHES_COUNT, HASHES_COUNT_PER_DEVICE[:]))
    if len(ss) > 7:
        ss.pop(0)
    average_time_diff = ss[-1][0] - ss[0][0]
    average_hash_diff = ss[-1][1] - ss[0][1]
    average_hash_rate = average_hash_diff / average_time_diff
    log_text = 'average hashrate: %.2fMH/s in %.2fs, %d shares found' % (
        (average_hash_rate / 10**6), average_time_diff, SHARES_COUNT
    )
    if POOL_HAS_RESULTS:  # once reported has reported
        log_text += ', %d accepted' % SHARES_ACCEPTED
    logging.info(log_text)

    if cnt >= 6 and cnt % 6 == 2:
        recent_time_diff = ss[-1][0] - ss[-2][0]
        recent_hash_diff = ss[-1][1] - ss[-2][1]
        recent_hash_rate = recent_hash_diff / recent_hash_diff
        logging.info('hashrate in last minute: %.2fMH/s in %.2fs' % (
            (recent_hash_rate / 10 ** 6), recent_time_diff
        ))
    return ss


def save_hash_rate_per_device_report(ss: list, cnt: int, start_time: int) -> None:
    if cnt < 8:  # compute recent
        start = ss[-2]
    elif cnt % 6 == 2:  # compute average
        start = ss[0]
    else:
        return
    end = ss[-1]

    time_diff = end[0] - start[0]
    rate_per_device = [
        (_b - _a) / time_diff / 10**6
        for _a, _b in zip(start[2], end[2])
    ]
    with open('stats.json', 'w') as f:
        json.dump({
            'total': (end[1] - start[1]) / time_diff / 10**3,
            'rates': rate_per_device,
            'uptime': time.time() - start_time,
            'accepted': SHARES_ACCEPTED,
            'rejected': SHARES_COUNT - SHARES_ACCEPTED,
        }, f)


def main() -> None:
    ss = [(time.time(), HASHES_COUNT, [0] * len(devices))]
    count = 0
    while True:
        time.sleep(10)
        count += 1
        ss = report_hash_rate(ss, count)
        if args.STATS:
            save_hash_rate_per_device_report(ss, count, start_time)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        sys.argv.append('')
    if sys.argv[1] == 'info':
        try:
            print_info(VERSION)
        except cl.LogicError:
            print('Failed to get OpenCL platforms, check your graphics card drivers')
            exit(0)
        else:
            exit(0)
    if sys.argv[1] == 'run':
        run_args = sys.argv[2:]
    elif sys.argv[1].startswith('http') or sys.argv[1].startswith('-'):
        run_args = sys.argv[1:]
    else:
        print_usage(VERSION)
        exit(0)

    parser = set_parser(argparse.ArgumentParser())
    args = parser.parse_args(run_args)

    set_logger(args)

    if args.STATS:
        start_time = time.time()

    try:
        test_pool_connection(VERSION, args.POOL, args.WALLET, HEADERS)
    except KeyError:
        logging.error(f'Please check your wallet address: {args.WALLET}')
        exit(1)
    except Exception as e:
        logging.exception(f'Failed to connect to pool: {e}')
        exit(1)

    start_receiver()
    start_reporter()

    try:
        devices = select_devices(args.PLATFORM, args.DEVICE)
    except PlatformIDError:
        logging.error(f'Wrong platform ID: {int(args.PLATFORM)}')
        exit(1)
    except DeviceIDError:
        logging.error(f'Wrong device ID: {int(args.DEVICE)}')
        if args.PLATFORM is not None and len(args.PLATFORM) > 1:
            logging.error('You may want to specify a platform ID')
        exit(1)

    logging.info(f'Total devices: {len(devices)}')
    HASHES_COUNT_PER_DEVICE = [0] * len(devices)  # for and from worker

    try:
        start_worker(devices, args.THREADS)
    except Exception:
        logging.info('Failed to load opencl program')
        exit(1)

    try:
        main()
    except KeyboardInterrupt:
        logging.info('Exiting...')
        exit(0)
