"""Real stress workers/TCP frames with only ZooKeeper discovery replaced."""
import argparse
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
import threading
import time
import re
import unittest

CLIENT = None
NO_ASLR = False

class StressClientTests(unittest.TestCase):
    def run_case(self, response_kind, args=None, expected_completed=6,
                 response_delay=0, expected_clients=2):
        with tempfile.TemporaryDirectory(prefix='rpc-stress-client-') as directory:
            work = Path(directory)
            (work / 'run').mkdir()
            config = work / 'client.json'
            config.write_text(json.dumps({'timeout_ms': 1000, 'retry_times': 0}))
            listener = socket.socket()
            listener.bind(('127.0.0.1', 0)); listener.listen(128)
            listener.settimeout(.1)
            stop = threading.Event()
            workers = []
            errors = []
            client_requests = []
            def receive(sock, size):
                data = bytearray()
                while len(data) < size:
                    chunk = sock.recv(size - len(data))
                    if not chunk: return None
                    data.extend(chunk)
                return bytes(data)
            def serve(sock, requests, index):
                with sock:
                    sock.settimeout(3)
                    try:
                        while not stop.is_set():
                            header = receive(sock, 12)
                            if header is None: return
                            magic, size, sequence = struct.unpack('!III', header)
                            body = receive(sock, size)
                            if body is None: return
                            offset = 0
                            for _ in range(2):
                                length = struct.unpack_from('!I', body, offset)[0]
                                offset += 4 + length
                            length = struct.unpack_from('!I', body, offset)[0]
                            request = json.loads(body[offset + 4:offset + 4 + length])
                            requests.append((request['request_id'], time.monotonic()))
                            delay = response_delay[index] if isinstance(response_delay, tuple) else response_delay
                            if delay: time.sleep(delay)
                            values = {
                                'valid': {'received_message': request['message']},
                                'number': {'received_message': 123},
                                'null': {'received_message': None},
                                'missing': {}, 'array': []}
                            result = json.dumps(values[response_kind]).encode()
                            response = struct.pack('!III', 0, 0, len(result)) + result
                            sock.sendall(struct.pack('!III', magic, len(response), sequence) + response)
                    except OSError:
                        return
                    except Exception as error:
                        errors.append(error)
            def accept():
                while not stop.is_set():
                    try: sock, _ = listener.accept()
                    except (TimeoutError, OSError): continue
                    requests = []
                    index = len(client_requests)
                    client_requests.append(requests)
                    thread = threading.Thread(target=serve, args=(sock, requests, index))
                    workers.append(thread); thread.start()
            acceptor = threading.Thread(target=accept); acceptor.start()
            env = os.environ.copy()
            env['RPC_STRESS_TEST_BACKEND'] = f'127.0.0.1:{listener.getsockname()[1]}'
            if args is None: args = ['--threads', '2', '--requests', '6']
            command = [CLIENT, '--config', str(config), *args]
            if NO_ASLR: command = ['setarch', os.uname().machine, '-R', *command]
            try:
                result = subprocess.run(command, cwd=work / 'run', env=env,
                                        capture_output=True, text=True, timeout=15)
            finally:
                stop.set(); acceptor.join(); listener.close()
                for worker in workers: worker.join()
            self.assertFalse(errors, errors)
            expected = 0 if response_kind == 'valid' else 1
            self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
            completed = sum(len(requests) for requests in client_requests)
            if expected_completed is not None: self.assertEqual(completed, expected_completed)
            self.assertEqual(len(client_requests), expected_clients)
            self.assertIn(f'Completed requests: {completed}', result.stdout)
            self.assertIn(f"Failed            : {0 if expected == 0 else completed}", result.stdout)
            self.assertNotIn('terminate called', result.stderr)
            self.assertNotIn('runtime error:', result.stderr)
            self.assertNotIn('Sanitizer', result.stderr)
            ids = [request_id for requests in client_requests for request_id, _ in requests]
            self.assertEqual(len(set(ids)), completed, 'duplicate request IDs')
            return result, client_requests

    def test_valid_and_malformed_echo_values_are_counted(self):
        for kind in ('valid', 'number', 'null', 'missing', 'array'):
            with self.subTest(kind=kind): self.run_case(kind)

    def test_default_clients_each_send_100_requests(self):
        _, requests = self.run_case('valid', args=[], expected_completed=1000, expected_clients=10)
        self.assertEqual([len(client) for client in requests], [100] * 10)

    def test_50_clients_each_send_200_requests(self):
        _, requests = self.run_case('valid',
            args=['--threads', '50', '--requests-per-client', '200'],
            expected_completed=10000, expected_clients=50)
        self.assertEqual([len(client) for client in requests], [200] * 50)

    def test_per_client_counts_do_not_depend_on_response_speed(self):
        _, requests = self.run_case('valid',
            args=['--threads', '3', '--requests-per-client', '4'],
            expected_completed=12, expected_clients=3, response_delay=(.02, .005, 0))
        self.assertEqual([len(client) for client in requests], [4] * 3)

    def test_duration_runs_until_deadline_and_drains_inflight_calls(self):
        result, requests = self.run_case('valid',
            args=['--threads', '2', '--duration', '1'], expected_completed=None, response_delay=.6)
        self.assertTrue(all(requests))
        elapsed = float(re.search(r'Elapsed\s+: ([\d.]+)', result.stdout).group(1))
        self.assertGreaterEqual(elapsed, 1.1)
        self.assertLess(elapsed, 4)
        starts = [stamp for client in requests for _, stamp in client]
        self.assertLess(max(starts) - min(starts), 1.1)
        self.assertIn('Percentiles       : approximate', result.stdout)
        p50 = float(re.search(r'P50 latency\s+: ([\d.]+)', result.stdout).group(1))
        self.assertGreaterEqual(p50, 600)
        self.assertLess(p50, 1000)

    def test_duration_is_not_capped_by_default_request_count_and_counts_errors(self):
        for kind in ('valid', 'number'):
            with self.subTest(kind=kind):
                _, requests = self.run_case(kind, args=['--threads', '2', '--duration', '1'],
                    expected_completed=None, response_delay=.005)
                self.assertGreater(sum(len(client) for client in requests), 100)

    def test_invalid_modes_fail_before_connecting(self):
        for args in (
            ['--duration', '0'], ['--duration', '86401'],
            ['--requests-per-client', '0'],
            ['--threads', '1024', '--requests-per-client', '10000'],
            ['--requests', '1', '--duration', '1'],
            ['--duration', '1', '--requests-per-client', '1']):
            with self.subTest(args=args):
                result = subprocess.run([CLIENT, *args], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertNotIn('Failed to connect', result.stdout + result.stderr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', required=True)
    parser.add_argument('--no-aslr', action='store_true')
    args, remaining = parser.parse_known_args()
    CLIENT = str(Path(args.client).resolve()); NO_ASLR = args.no_aslr
    unittest.main(argv=[__file__, *remaining])
