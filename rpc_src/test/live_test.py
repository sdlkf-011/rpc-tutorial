"""验证真实 ZooKeeper 会话过期、重启发现、TCP 背压和压缩加密调用。

需要 Java 和 ZooKeeper 服务端 jars，可通过 ZK_SERVER_CLASSPATH 指定安装位置。
TCP 代理只隔离注册会话，保留 ZooKeeper 服务端和观察者连接，
使会话过期由真实 ZooKeeper 服务端判断。
"""
import argparse
import json
import os
from pathlib import Path
import queue
import select
import shutil
import socket
import subprocess
import tempfile
import threading
import time

class Proxy:
    def __init__(self, backend):
        self.backend = backend
        self.socket = socket.socket()
        self.socket.bind(('127.0.0.1', 0)); self.socket.listen()
        self.socket.settimeout(.1)
        self.port = self.socket.getsockname()[1]
        self.condition = threading.Condition()
        self.paused = self.stopped = self.blocked = False
        self.parked = 0
        self.connections = set()
        self.workers = []
        self.acceptor = threading.Thread(target=self.accept)
        self.acceptor.start()

    def accept(self):
        while not self.stopped:
            try: client, _ = self.socket.accept()
            except (OSError, TimeoutError): continue
            with self.condition:
                self.connections.add(client)
            thread = threading.Thread(target=self.forward, args=(client,))
            self.workers.append(thread); thread.start()

    def forward(self, client):
        upstream = None
        try:
            with self.condition:
                self.condition.wait_for(lambda: not self.paused or self.stopped)
                if self.stopped: return
                upstream = socket.create_connection(('127.0.0.1', self.backend), timeout=1)
                self.connections.add(upstream)
            client.setblocking(False); upstream.setblocking(False)
            pending = {client: bytearray(), upstream: bytearray()}
            peers = {client: upstream, upstream: client}
            while not self.stopped:
                with self.condition:
                    if self.blocked:
                        self.parked += 1; self.condition.notify_all()
                        self.condition.wait_for(lambda: not self.blocked or self.stopped)
                        self.parked -= 1
                    if self.stopped: return
                readable, writable, _ = select.select([client, upstream],
                    [sock for sock, data in pending.items() if data], [], .1)
                for sock in readable:
                    try: data = sock.recv(65536)
                    except BlockingIOError: continue
                    if not data: return
                    pending[peers[sock]].extend(data)
                for sock in writable:
                    try: sent = sock.send(pending[sock])
                    except BlockingIOError: continue
                    del pending[sock][:sent]
        except OSError:
            pass
        finally:
            with self.condition:
                self.connections.discard(client)
                if upstream: self.connections.discard(upstream)
            client.close()
            if upstream: upstream.close()

    def stall(self):
        # 保持连接但停止转发，模拟服务端迟迟不返回注册结果。
        with self.condition:
            self.blocked = True
            if not self.condition.wait_for(lambda: self.parked > 0, timeout=2):
                raise RuntimeError('registry proxy did not stall')

    def unstall(self):
        with self.condition:
            self.blocked = False; self.condition.notify_all()

    def pause(self):
        # 切断已有连接并阻止重连，使注册会话真正超时过期。
        with self.condition:
            self.paused = True
            for sock in list(self.connections):
                try: sock.shutdown(socket.SHUT_RDWR)
                except OSError: pass

    def resume(self):
        with self.condition:
            self.paused = False; self.condition.notify_all()

    def close(self):
        with self.condition:
            self.stopped = True; self.condition.notify_all()
            for sock in list(self.connections):
                try: sock.shutdown(socket.SHUT_RDWR)
                except OSError: pass
        self.acceptor.join()
        self.socket.close()
        for worker in self.workers: worker.join()

class ZooKeeper:
    """在独立数据目录和本地随机端口启动 ZooKeeper，不占用现有实例。"""
    def __init__(self, work, classpath):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); self.port = sock.getsockname()[1]
        self.log = (work / 'zookeeper.log').open('w')
        self.config = work / 'zoo.cfg'
        self.config.write_text(f'tickTime=500\nminSessionTimeout=1000\nmaxSessionTimeout=2000\n'
            f'dataDir={work / "data"}\nclientPort={self.port}\nclientPortAddress=127.0.0.1\nadmin.enableServer=false\n')
        self.classpath = classpath
        self.process = None

    def start(self):
        self.process = subprocess.Popen(['java', '-Dzookeeper.nio.numWorkerThreads=4', '-cp', self.classpath,
            'org.apache.zookeeper.server.quorum.QuorumPeerMain', str(self.config)], stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None: raise RuntimeError('ZooKeeper startup failed')
            try:
                with socket.create_connection(('127.0.0.1', self.port), timeout=.1): return
            except OSError: time.sleep(.1)
        raise RuntimeError('ZooKeeper startup timed out')

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(timeout=5)
            except subprocess.TimeoutExpired: self.process.kill(); self.process.wait()

    def close(self): self.stop(); self.log.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--driver', required=True)
    parser.add_argument('--artifacts')
    parser.add_argument('--no-aslr', action='store_true', help='disable ASLR for a TSan driver on affected Linux kernels')
    args = parser.parse_args()
    classpath = os.environ.get('ZK_SERVER_CLASSPATH', '/usr/share/java/zookeeper.jar:/usr/share/java/slf4j-simple.jar')
    if not shutil.which('java') or not all(Path(part).exists() for part in classpath.split(os.pathsep)):
        print('SKIP: Java/ZooKeeper jars unavailable; set ZK_SERVER_CLASSPATH')
        return 77
    temporary = tempfile.TemporaryDirectory(prefix='rpc-live-')
    work = Path(args.artifacts).resolve() if args.artifacts else Path(temporary.name)
    work.mkdir(parents=True, exist_ok=True)
    keeper = ZooKeeper(work, classpath)
    proxy = driver = None
    events = queue.Queue()
    log = (work / 'driver.log').open('w')
    reader = None
    try:
        keeper.start(); proxy = Proxy(keeper.port)
        driver_command = [str(Path(args.driver).resolve()), f'127.0.0.1:{keeper.port}',
                          f'127.0.0.1:{proxy.port}', str(work)]
        if args.no_aslr:
            driver_command = ['setarch', os.uname().machine, '-R', *driver_command]
        driver = subprocess.Popen(driver_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True)
        def collect():
            for line in driver.stdout:
                log.write(line); log.flush()
                if line.startswith('@rpc-live '): events.put(json.loads(line[len('@rpc-live '):]))
            events.put({'event': 'driver-exit'})
        reader = threading.Thread(target=collect); reader.start()
        def expect(event, command=True):
            if command: driver.stdin.write(event + '\n'); driver.stdin.flush()
            record = events.get(timeout=25)
            if record['event'] != event: raise RuntimeError(f'expected {event}, got {record}; inspect {work}')
            print('LIVE PASS', event, record, flush=True)
            return record
        initial = expect('ready', False)
        expect('tcp')
        # 真实发现链路下检查传输变换以及客户端销毁后的异步结果。
        expect('transport')
        proxy.stall(); expect('registration-deadline')
        proxy.unstall(); initial = expect('deadline-recovered')
        proxy.pause(); expect('absent')
        proxy.resume(); recovered = expect('recovered')
        if recovered['owner'] == initial['owner']: raise RuntimeError('expired session owner did not change')
        keeper.stop(); time.sleep(3); keeper.start(); expect('available')
        expect('restart-tcp')
        driver.stdin.write('stop\n'); driver.stdin.flush()
        expect('stopped', False)
        driver.wait(timeout=10)
        if driver.returncode: raise RuntimeError('driver failed during cleanup')
        return 0
    finally:
        if driver and driver.poll() is None:
            driver.terminate()
            try: driver.wait(timeout=5)
            except subprocess.TimeoutExpired: driver.kill(); driver.wait()
        if reader: reader.join(timeout=5)
        if proxy: proxy.close()
        keeper.close(); log.close(); temporary.cleanup()

if __name__ == '__main__':
    raise SystemExit(main())
