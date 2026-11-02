"""Bounded existing-Node Brotli bridge; no credentials, dependencies or fetch on import."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time


def fetch_skinport_cash(exact_hashes):
    if (not isinstance(exact_hashes, list) or not 1 <= len(exact_hashes) <= 10000
            or any(not isinstance(name, str) or not name or len(name) > 512 for name in exact_hashes)
            or len(set(exact_hashes)) != len(exact_hashes)):
        raise ValueError('A bounded deduplicated exact market-hash set is required')
    binary = os.environ.get('CS2_CASH_NODE') or shutil.which('node')
    if not binary or not Path(binary).is_file():
        return {'status': None, 'receivedAtUs': time.time_ns() // 1000,
                'error': 'Existing Node runtime is unavailable for required Brotli decoding'}
    helper = Path(__file__).resolve().with_suffix('.mjs')
    request = json.dumps(exact_hashes, ensure_ascii=False, allow_nan=False).encode('utf-8')
    if len(request) > 1048576:
        raise ValueError('Exact market-hash input exceeded cap')
    try:
        process = subprocess.Popen([binary, '--max-old-space-size=128', str(helper)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    except OSError:
        return {'status': None, 'receivedAtUs': time.time_ns() // 1000,
                'error': 'Cash source transport could not start its existing runtime'}
    stdout, stderr = bytearray(), bytearray()
    failed = threading.Event()

    def read_bounded(pipe, target, maximum):
        try:
            while True:
                chunk = pipe.read(65536)
                if not chunk:
                    break
                if len(target) + len(chunk) > maximum:
                    failed.set()
                    break
                target.extend(chunk)
        except (OSError, ValueError):
            failed.set()
        finally:
            pipe.close()

    def write_input():
        try:
            process.stdin.write(request)
            process.stdin.flush()
        except (OSError, ValueError):
            failed.set()
        finally:
            process.stdin.close()

    threads = [threading.Thread(target=read_bounded, args=(process.stdout, stdout, 8 * 1024 * 1024), daemon=True),
               threading.Thread(target=read_bounded, args=(process.stderr, stderr, 65536), daemon=True),
               threading.Thread(target=write_input, daemon=True)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 25
    stopped_for_bound = False
    while process.poll() is None:
        if failed.is_set() or time.monotonic() >= deadline:
            stopped_for_bound = True
            try:
                process.kill()
            except OSError:
                pass
            break
        time.sleep(0.02)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        return {'status': None, 'receivedAtUs': time.time_ns() // 1000,
                'error': 'Cash source transport cleanup is unconfirmed', 'transportClosed': False}
    for thread in threads:
        thread.join(timeout=1)
    if (process.returncode or stopped_for_bound or failed.is_set()
            or any(thread.is_alive() for thread in threads)):
        return {'status': None, 'receivedAtUs': time.time_ns() // 1000,
                'error': 'Cash source transport failed its process, deadline or bounded pipe read',
                'transportClosed': True}
    try:
        result = json.loads(stdout)
    except (ValueError, UnicodeError):
        return {'status': None, 'receivedAtUs': time.time_ns() // 1000,
                'error': 'Cash source transport returned invalid JSON'}
    if not isinstance(result, dict):
        return {'status': None, 'receivedAtUs': time.time_ns() // 1000,
                'error': 'Cash source transport returned an invalid result shape'}
    return result
