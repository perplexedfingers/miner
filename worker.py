import hashlib
import json
import logging
import time
from queue import Queue
from threading import RLock

import numpy as np
import pyopencl as cl

TASK_LOCK = RLock()
CUR_TASK = None  # from receiver
HASHES_LOCK = RLock()
HASHES_COUNT_PER_DEVICE = []  # for miner
SHARE_REPORT_QUEUE = Queue()  # for reporter
HASHES_COUNT = 0  # for miner and receiver
HASHES_COUNT_DEVFEE = 0  # for receiver
BENCHMARK_LOCK = RLock()  # might not need this?

try:
    BENCHMARK_DATA = json.load(open('benchmark_data.txt'))
except Exception:
    BENCHMARK_DATA = {}


def get_device_id(device):
    name = device.name
    try:
        bus = device.get_info(0x4008)
        slot = device.get_info(0x4009)
        return name + ' on PCI bus %d slot %d' % (bus, slot)
    except cl.LogicError:
        pass
    try:
        topo = device.get_info(0x4037)
        return name + ' on PCI bus %d device %d function %d' % (topo.bus, topo.device, topo.function)
    except cl.LogicError:
        pass
    return name


def report_benchmark(id, data):
    with BENCHMARK_LOCK:
        BENCHMARK_DATA[id] = data
        json.dump(BENCHMARK_DATA, open('benchmark_data.txt', 'w'))


def count_hashes(num, device_id, count_devfee):
    global HASHES_COUNT, HASHES_COUNT_DEVFEE
    with HASHES_LOCK:
        HASHES_COUNT += num
        HASHES_COUNT_PER_DEVICE[device_id] += num
        if count_devfee:
            HASHES_COUNT_DEVFEE += num


def get_task(iterations):
    with TASK_LOCK:
        global_it, input, giver, complexity, hash_state, suffix_arr, tm, submit_conf, count_devfee = CUR_TASK
        CUR_TASK[0] += 256
    suffix_np = np.array(suffix_arr[:12] + [suffix_arr[14]]).astype(np.uint32)
    return (input, giver, complexity, suffix_arr, global_it, tm, submit_conf, count_devfee,
            np.concatenate((np.array([iterations, global_it]).astype(np.uint32), hash_state, suffix_np)))


class Worker:
    def __init__(self, device, program, threads, id):
        self.device = device
        self.device_id = id
        self.context = cl.Context(devices=[device], dev_type=None)
        self.queue = cl.CommandQueue(self.context)
        self.program = cl.Program(self.context, program).build()
        self.kernels = self.program.all_kernels()
        if threads is None:
            threads = device.max_compute_units * device.max_work_group_size
            if device.type & 4 == 0:
                threads = device.max_work_group_size
        self.threads = threads

    def run_task(self, kernel, iterations):
        mf = cl.mem_flags
        input, giver, complexity, suffix_arr, global_it, tm, submit_conf, count_devfee, args = get_task(iterations)
        # input, complexity, suffix_arr, global_it, and args are needed for mining. others are metadata
        args_g = cl.Buffer(self.context, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=args)
        res_g = cl.Buffer(self.context, mf.WRITE_ONLY | mf.COPY_HOST_PTR, hostbuf=np.full(2048, 0xffffffff, np.uint32))
        kernel(self.queue, (self.threads,), None, args_g, res_g)
        res = np.empty(2048, np.uint32)
        e = cl.enqueue_copy(self.queue, res, res_g, is_blocking=False)  # just set blocking?

        while e.get_info(cl.event_info.COMMAND_EXECUTION_STATUS) != cl.command_execution_status.COMPLETE:
            time.sleep(0.1)

        os = list(np.where(res != 0xffffffff))[0]
        if len(os):
            for j in range(0, len(os), 2):
                p = os[j]
                assert os[j + 1] == p + 1
                a = res[p]
                b = res[p + 1]
                suf = suffix_arr[:]
                suf[0] ^= b
                suf[12] ^= b
                suf[1] ^= a
                suf[13] ^= a
                suf[2] ^= global_it
                suf[14] ^= global_it
                input_new = input[:64]
                for x in suf:
                    input_new += int(x).to_bytes(4, 'big')
                h = hashlib.sha256(input_new[:123]).digest()
                if h[:4] != b'\0\0\0\0':
                    logging.warning('hash integrity error, please check your graphics card drivers')
                if h < complexity:
                    SHARE_REPORT_QUEUE.put((input_new[:123].hex(), giver, h, tm, submit_conf))
        count_hashes(self.threads * iterations, self.device_id, count_devfee)

    def warmup(self, kernel, time_limit):
        iterations = 4096
        st = time.time()
        while True:
            ct = time.time()
            self.run_task(kernel, iterations)
            elapsed = time.time() - ct
            if elapsed < 0.7:
                iterations *= 2
            if time.time() - st > time_limit:
                break
        return iterations, elapsed

    def benchmark_kernel(self, dd, kernel, report_status):
        iterations = 2048
        max_hr = (0, 0)
        flag = False
        while True:
            iterations *= 2
            st = time.time()
            cnt = 0
            while True:
                ct = time.time()
                self.run_task(kernel, iterations)
                if time.time() - ct > 3:
                    flag = True
                    break
                cnt += 1
                if cnt >= 4 and time.time() - st > 2:
                    break
            if flag:
                break
            report_status(iterations)
            hs = cnt * self.threads * iterations
            tm = time.time() - st
            hr = hs / tm
            logging.debug('benchmark data: %s %d iterations %.2fMH/s (%d hashes in %ss)' % (kernel.function_name, iterations, hr / 1e6, hs, tm))
            if hr > max_hr[0]:
                max_hr = (hr, iterations)
        report_benchmark(dd + ':' + kernel.function_name, list(max_hr))

    def find_kernel(self, kernel_name):
        for kernel in self.kernels:
            if kernel.function_name == kernel_name:
                return kernel
        return self.kernels[0]

    def run_benchmark(self, kernels):
        def show_benchmark_status(x):
            nonlocal old_benchmark_status
            x = x * 100
            if x > old_benchmark_status + 2 and x <= 98:
                old_benchmark_status = x
                logging.info('benchmarking %s ... %d%%' % (dd, int(x)))

        def report_benchmark_status(it):
            nonlocal cur
            if it in ut:
                cur += ut[it]
                show_benchmark_status(cur / tot)
        dd = get_device_id(self.device)
        logging.info('starting benchmark for %s ...' % dd)
        logging.info('the hashrate may be not stable in several minutes due to benchmarking')
        old_benchmark_status = 0
        it, el = self.warmup(self.find_kernel('hash_solver_3'), 15)
        el /= it
        ut = {}
        show_benchmark_status(15 / (len(kernels) * 40 + 20))
        for i in range(12, 100):
            t = 2**i * el
            if t > 4:
                break
            ut[2**i] = max(t * 4, 2.2)
        tot = (15 + sum(ut.values()) * 4) / 0.95
        cur = 15
        for kernel in kernels:
            self.benchmark_kernel(dd, kernel, report_benchmark_status)

    def run(self):
        dd = get_device_id(self.device)
        pending_benchmark = [
            kernel for kernel in self.kernels
            if dd + ':' + kernel.function_name not in BENCHMARK_DATA
        ]
        if len(pending_benchmark):
            self.run_benchmark(pending_benchmark)
        max_hr = 0
        for kernel in self.kernels:
            hr, it = BENCHMARK_DATA[dd + ':' + kernel.function_name]
            if hr > max_hr:
                max_hr = hr
                self.best_kernel = kernel
                self.iterations = it
        logging.info('%s: starting normal mining with %s and %d iterations per thread' % (dd, self.best_kernel.function_name, self.iterations))
        while True:
            self.run_task(self.best_kernel, self.iterations)
