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
import unittest

CLIENT = None
NO_ASLR = False

class StressClientTests(unittest.TestCase):
    def run_case(self, response_kind):
        with tempfile.TemporaryDirectory(prefix='rpc-stress-client-') as directory:
            work = Path(directory)
            (work / 'run').mkdir()
            config = work / 'client.json'
            config.write_text(json.dumps({'timeout_ms': 1000, 'retry_times': 0}))
            listener = socket.socket()
            listener.bind(('127.0.0.1', 0)); listener.listen(8)
            listener.settimeout(.1)
            stop = threading.Event()
            workers = []
            errors = []
            def receive(sock, size):
                data = bytearray()
                while len(data) < size:
                    chunk = sock.recv(size - len(data))
                    if not chunk: return None
                    data.extend(chunk)
                return bytes(data)
            def serve(sock):
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
                    thread = threading.Thread(target=serve, args=(sock,))
                    workers.append(thread); thread.start()
            acceptor = threading.Thread(target=accept); acceptor.start()
            env = os.environ.copy()
            env['RPC_STRESS_TEST_BACKEND'] = f'127.0.0.1:{listener.getsockname()[1]}'
            command = [CLIENT, '--config', str(config), '--threads', '2', '--requests', '6']
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
            self.assertIn('Completed requests: 6', result.stdout)
            self.assertIn(f"Failed            : {0 if expected == 0 else 6}", result.stdout)
            self.assertNotIn('terminate called', result.stderr)
            self.assertNotIn('runtime error:', result.stderr)
            self.assertNotIn('Sanitizer', result.stderr)

    def test_valid_and_malformed_echo_values_are_counted(self):
        for kind in ('valid', 'number', 'null', 'missing', 'array'):
            with self.subTest(kind=kind): self.run_case(kind)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', required=True)
    parser.add_argument('--no-aslr', action='store_true')
    args, remaining = parser.parse_known_args()
    CLIENT = str(Path(args.client).resolve()); NO_ASLR = args.no_aslr
    unittest.main(argv=[__file__, *remaining])
