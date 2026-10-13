"""Run the documented startup commands from a project root with real ZooKeeper."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from live_test import ZooKeeper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--client', type=Path, required=True)
    parser.add_argument('--stress-client', type=Path, required=True)
    parser.add_argument('--config-dir', type=Path, required=True)
    parser.add_argument('--zk-launcher', type=Path, required=True)
    parser.add_argument('--stress-script', type=Path, required=True)
    args = parser.parse_args()
    classpath = os.environ.get(
        'ZK_SERVER_CLASSPATH',
        '/usr/share/java/zookeeper.jar:/usr/share/java/slf4j-simple.jar')
    if not shutil.which('java') or not all(
            Path(part).exists() for part in classpath.split(os.pathsep)):
        print('SKIP: Java/ZooKeeper jars unavailable; set ZK_SERVER_CLASSPATH')
        return 77

    with tempfile.TemporaryDirectory(prefix='rpc-startup-') as directory:
        project = Path(directory)
        build = project / 'build'
        config_dir = project / 'config'
        keeper_dir = project / 'zk'
        for path in (build, config_dir, keeper_dir):
            path.mkdir()
        for binary in (args.server, args.client, args.stress_client):
            shutil.copy2(binary, build / binary.name)
        shutil.copy2(args.zk_launcher, project / 'zk_start.sh')
        shutil.copy2(args.stress_script, project / 'stress_test.sh')
        keeper = ZooKeeper(keeper_dir, classpath)
        server = None
        try:
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            server_config = json.loads((args.config_dir / 'rpc_server.json').read_text())
            client_config = json.loads((args.config_dir / 'rpc_client.json').read_text())
            registry_config = json.loads((args.config_dir / 'servers.json').read_text())
            server_config.update(servers_ip='127.0.0.1', servers_port=port)
            server_config['scheduler'].update(zk_host='127.0.0.1', zk_port=keeper.port)
            client_config.update(zk_host='127.0.0.1', zk_port=keeper.port, server_port=port)
            registry_config['registry_nodes'] = [{'address': '127.0.0.1', 'port': port}]
            for filename, config in (
                    ('rpc_server.json', server_config),
                    ('rpc_client.json', client_config),
                    ('servers.json', registry_config)):
                (config_dir / filename).write_text(json.dumps(config))
            zoo_config = (args.config_dir / 'zoo.cfg').read_text()
            (config_dir / 'zoo.cfg').write_text(
                zoo_config.replace('clientPort=2181', f'clientPort={keeper.port}'))
            env = os.environ.copy()
            env['ZK_SERVER_CLASSPATH'] = classpath
            env['JAVA_TOOL_OPTIONS'] = '-Dzookeeper.nio.numWorkerThreads=4'
            keeper.start(command=['./zk_start.sh'], cwd=project, env=env)
            log_path = project / 'server.log'
            with log_path.open('w') as log:
                server = subprocess.Popen(['./build/' + args.server.name], cwd=project,
                                          stdout=log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    output = log_path.read_text()
                    if 'RPC server started on ' in output:
                        break
                    if server.poll() is not None:
                        raise RuntimeError(output)
                    time.sleep(.05)
                else:
                    raise RuntimeError('Server startup timed out: ' + output)
                result = subprocess.run(['./build/' + args.client.name], cwd=project,
                                        capture_output=True, text=True, timeout=15)
                assert result.returncode == 0, result.stdout + result.stderr
                assert '"received_message": "Hello, cookrpc"' in result.stdout, result.stdout
                result = subprocess.run(
                    ['./build/' + args.stress_client.name, '--threads', '1', '--requests', '2'],
                    cwd=project, capture_output=True, text=True, timeout=15)
                assert result.returncode == 0, result.stdout + result.stderr
                assert 'Successful        : 2' in result.stdout, result.stdout
                # 直接运行用户的脚本入口，验证次数模式和时长模式完整调用链。
                stress_env = os.environ.copy()
                for key in ('BUILD_DIR', 'BUILD_FIRST', 'START_SERVER', 'SERVER_LOG',
                            'CLIENT_CONFIG', 'SERVER_CONFIG', 'THREADS', 'REQUESTS',
                            'REQUESTS_PER_CLIENT', 'DURATION', 'PAYLOAD_SIZE'):
                    stress_env.pop(key, None)
                for cli_args, expected_count in (
                        ([], 1000),
                        (['-c', '50', '-r', '200'], 10000),
                        (['-c', '20', '-t', '1'], None)):
                    result = subprocess.run(['./stress_test.sh', *cli_args], cwd=project,
                        env=stress_env, capture_output=True, text=True, timeout=30)
                    assert result.returncode == 0, result.stdout + result.stderr
                    assert 'Failed            : 0' in result.stdout, result.stdout
                    if expected_count is not None:
                        assert f'Completed requests: {expected_count}' in result.stdout, result.stdout
                    else:
                        assert 'Duration target   : 1 s' in result.stdout, result.stdout
                    print(f'PASS: stress script {cli_args or "defaults"}')
            assert list((project / 'logs').glob('cookrpc_*.log')), 'Missing project logs'
        finally:
            try:
                if server and server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()
                        raise RuntimeError('RPC server shutdown timed out')
            finally:
                keeper.close()
        assert server.returncode == 0, log_path.read_text()
        print('PASS: project-root startup, default configuration and Echo calls')
    return 0


if __name__ == '__main__':
    sys.exit(main())
