"""Exercise script argv/cwd/cache behavior with local fake programs, no SDK download."""
import json
import os
import shutil
import signal
import time
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]

class ScriptTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="rpc-scripts-")
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.env = os.environ.copy()
        for key in ("BUILD_DIR", "BUILD_FIRST", "START_SERVER", "SERVER_LOG", "CLIENT_CONFIG", "SERVER_CONFIG",
                    "VCPKG_ROOT", "VCPKG_TARGET_TRIPLET", "VCPKG_DEFAULT_TRIPLET", "CMAKE_TOOLCHAIN_FILE",
                    "SHUTDOWN_TIMEOUT_SECONDS", "ZOOKEEPER_HOME", "ZK_SERVER_CLASSPATH",
                    "THREADS", "REQUESTS", "REQUESTS_PER_CLIENT", "DURATION", "PAYLOAD_SIZE"):
            self.env.pop(key, None)

    def program(self, path, code):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/usr/bin/env python3\n" + code)
        path.chmod(0o755)

    def run_script(self, name, args=(), env=None):
        return subprocess.run(["bash", str(ROOT / name), *args], cwd=self.work,
                              env=env or self.env, capture_output=True, text=True, timeout=15)

    def test_zookeeper_launcher_supports_relocated_projects_and_install_paths(self):
        project = self.work / 'relocated project'
        project.mkdir()
        (project / 'config').mkdir()
        shutil.copy2(ROOT / 'zk_start.sh', project / 'zk_start.sh')
        shutil.copy2(ROOT / 'config/zoo.cfg', project / 'config/zoo.cfg')
        original_config = (project / 'config/zoo.cfg').read_text()
        tools = self.work / 'tools'
        trace = self.work / 'java.json'
        self.program(tools / 'java', """import json, os, pathlib, sys
pathlib.Path(os.environ['TRACE']).write_text(json.dumps({'argv': sys.argv[1:], 'cwd': os.getcwd()}))
""")
        self.env.update(PATH=str(tools) + os.pathsep + self.env['PATH'], TRACE=str(trace))
        sdk = self.work / 'zk sdk'
        (sdk / 'lib').mkdir(parents=True)
        (sdk / 'lib/zookeeper-test.jar').touch()
        for use_classpath in (False, True):
            with self.subTest(explicit_classpath=use_classpath):
                env = self.env.copy()
                if use_classpath:
                    env['ZK_SERVER_CLASSPATH'] = 'zk sdk/*:zk sdk/lib/*'
                else:
                    env['ZOOKEEPER_HOME'] = 'zk sdk'
                result = subprocess.run(['bash', str(project / 'zk_start.sh')],
                    cwd=self.work, env=env, capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                record = json.loads(trace.read_text())
                self.assertEqual(record['cwd'], str(project))
                argv = record['argv']
                self.assertEqual(argv[argv.index('-cp') + 1], f'{sdk}/*:{sdk}/lib/*')
                self.assertIn('org.apache.zookeeper.server.quorum.QuorumPeerMain', argv)
                self.assertEqual(argv[-1], str(project / 'config/zoo.cfg'))
                self.assertTrue((project / '.cache/zk-local/data').is_dir())
                self.assertEqual((project / 'config/zoo.cfg').read_text(), original_config)
                self.assertFalse((self.work / '.cache').exists())

    def test_zookeeper_help_and_invalid_install_do_not_start_java(self):
        tools = self.work / 'tools'
        self.program(tools / 'java', 'raise RuntimeError("Java must not start")\n')
        self.env['PATH'] = str(tools) + os.pathsep + self.env['PATH']
        self.env['ZOOKEEPER_HOME'] = str(self.work / 'missing distribution')
        result = self.run_script('zk_start.sh', ['--help'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Usage:', result.stdout)
        result = self.run_script('zk_start.sh')
        self.assertEqual(result.returncode, 1)
        self.assertIn('ZOOKEEPER_HOME must contain', result.stderr)

    def test_stress_resolves_cli_configs_before_cd_and_normalizes_build_and_log(self):
        build = self.work / "out build"
        client = self.work / "config files/client.json"
        server = self.work / "config files/server.json"
        client.parent.mkdir(); client.write_text("{}"); server.write_text("{}")
        trace = self.work / "argv.json"
        self.program(build / "stress_test_client", """import json, os, pathlib, sys
config = sys.argv[sys.argv.index('--config') + 1]
assert pathlib.Path(config).is_absolute() and pathlib.Path(config).is_file()
pathlib.Path(os.environ['TRACE']).write_text(json.dumps({'argv': sys.argv, 'cwd': os.getcwd()}))
""")
        self.program(build / "rpc_server", """import pathlib, signal, sys, time
assert pathlib.Path(sys.argv[1]).is_absolute() and pathlib.Path(sys.argv[1]).is_file()
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
print('RPC server started', flush=True)
while True: time.sleep(.1)
""")
        self.env.update(BUILD_DIR="out build", SERVER_LOG="logs here/server.log", TRACE=str(trace))
        result = self.run_script("stress_test.sh", ["--start-server", "--client-config", str(client.relative_to(self.work)),
                                                   "--server-config", str(server.relative_to(self.work))])
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(trace.read_text())
        self.assertEqual(record['cwd'], str(ROOT)); self.assertIn(str(client), record['argv'])
        self.assertTrue((self.work / "logs here/server.log").is_file())

    def test_stress_resolves_environment_config_from_the_caller_directory(self):
        build = self.work / "build"
        client = self.work / "client.json"; client.write_text("{}")
        self.program(build / "stress_test_client", """import pathlib, sys
path = pathlib.Path(sys.argv[sys.argv.index('--config') + 1])
assert path.is_absolute() and path.read_text() == '{}'
""")
        self.env.update(BUILD_DIR="build", CLIENT_CONFIG="client.json")
        result = self.run_script("stress_test.sh")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_stress_short_options_defaults_and_legacy_modes(self):
        build = self.work / 'stress modes'
        config = self.work / 'client.json'; config.write_text('{}')
        trace = self.work / 'stress-argv.json'
        self.program(build / 'stress_test_client', """import json, os, pathlib, sys
pathlib.Path(os.environ['TRACE']).write_text(json.dumps(sys.argv[1:]))
""")
        self.env['TRACE'] = str(trace)
        cases = [
            ([], {}, '10', '--requests-per-client', '100'),
            (['-c', '50', '-r', '200'], {}, '50', '--requests-per-client', '200'),
            (['-c', '20', '-t', '120'], {}, '20', '--duration', '120'),
            (['--threads', '8', '--requests', '50000'], {}, '8', '--requests', '50000'),
            ([], {'THREADS': '3', 'REQUESTS_PER_CLIENT': '7'}, '3', '--requests-per-client', '7'),
            ([], {'REQUESTS': '12'}, '10', '--requests', '12'),
            ([], {'DURATION': '2'}, '10', '--duration', '2'),
            (['-r', '4'], {'DURATION': '2'}, '10', '--requests-per-client', '4'),
        ]
        for args, overrides, clients, mode, value in cases:
            with self.subTest(args=args, env=overrides):
                env = self.env.copy(); env.update(overrides)
                result = self.run_script('stress_test.sh',
                    ['--build-dir', str(build), '--client-config', str(config), *args], env)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                argv = json.loads(trace.read_text())
                self.assertEqual(argv[argv.index('--threads') + 1], clients)
                self.assertEqual(argv[argv.index(mode) + 1], value)
                self.assertEqual(sum(flag in argv for flag in
                    ('--duration', '--requests', '--requests-per-client')), 1)

    def test_stress_rejects_invalid_or_conflicting_modes_before_starting_processes(self):
        build = self.work / 'invalid modes'
        trace = self.work / 'started'
        self.program(build / 'stress_test_client',
                     f'from pathlib import Path\nPath({str(trace)!r}).touch()\n')
        cases = [
            ['-r', '2', '-t', '1'], ['-t', '1', '-r', '2'],
            ['--requests', '10', '-r', '2'], ['--requests', '10', '-t', '1'],
            ['-c', '0'], ['-c', '1025'], ['-c', '-1'], ['-r', '1.5'],
            ['-r', '0'], ['-r', '999999999999999999999'],
            ['-c', '1024', '-r', '10000'], ['-t', '0'], ['-t', '86401'],
        ]
        for args in cases:
            with self.subTest(args=args):
                result = self.run_script('stress_test.sh', ['--build-dir', str(build), *args])
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(trace.exists(), 'invalid arguments started a client')

    def test_stress_cancellation_stops_owned_process_groups_and_reports_signal(self):
        for sig, ignored in ((signal.SIGTERM, False), (signal.SIGINT, False),
                             (signal.SIGTERM, True)):
            with self.subTest(signal=sig, ignores_term=ignored):
                build = self.work / ('cancel-' + str(sig) + '-' + str(ignored))
                client_config = self.work / 'cancel-client.json'
                server_config = self.work / 'cancel-server.json'
                client_config.write_text('{}'); server_config.write_text('{}')
                client_trace = build / 'client.json'; server_trace = build / 'server.json'
                self.program(build / 'rpc_server', """import json, os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
pathlib.Path(os.environ['SERVER_TRACE']).write_text(json.dumps({'pid': os.getpid(), 'group': os.getpgrp()}))
print('RPC server started', flush=True)
while True: time.sleep(.1)
""")
                self.program(build / 'stress_test_client', """import json, os, pathlib, signal, subprocess, sys, time
ignored = os.environ['IGNORE_TERM'] == '1'
child_code = 'import signal,time; ' + ('signal.signal(signal.SIGTERM, signal.SIG_IGN); ' if ignored else '') + 'time.sleep(30)'
child = subprocess.Popen([sys.executable, '-c', child_code])
def stop(*_):
    child.terminate()
    try: child.wait(timeout=1)
    except subprocess.TimeoutExpired: child.kill(); child.wait()
    sys.exit(0)
signal.signal(signal.SIGTERM, signal.SIG_IGN if ignored else stop)
pathlib.Path(os.environ['CLIENT_TRACE']).write_text(json.dumps({'pid': os.getpid(), 'group': os.getpgrp(), 'child': child.pid}))
while True: time.sleep(.1)
""")
                env = self.env.copy()
                env.update(CLIENT_TRACE=str(client_trace), SERVER_TRACE=str(server_trace),
                           IGNORE_TERM='1' if ignored else '0', SHUTDOWN_TIMEOUT_SECONDS='1')
                command = ['bash', str(ROOT / 'stress_test.sh'), '--start-server',
                           '--build-dir', str(build), '--client-config', str(client_config),
                           '--server-config', str(server_config), '--server-log', str(build / 'server.log')]
                process = subprocess.Popen(command, env=env, cwd=self.work,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                           start_new_session=True)
                records = []
                try:
                    deadline = time.monotonic() + 4
                    while not client_trace.exists() and time.monotonic() < deadline:
                        self.assertIsNone(process.poll())
                        time.sleep(.01)
                    self.assertTrue(client_trace.exists(), 'client did not start')
                    records = [json.loads(path.read_text()) for path in (server_trace, client_trace)]
                    start = time.monotonic()
                    process.send_signal(sig)
                    self.assertEqual(process.wait(timeout=4), 128 + sig)
                    self.assertLess(time.monotonic() - start, 4)
                    for record in records:
                        for pid in (record['pid'], record.get('child')):
                            if pid is None: continue
                            state = Path('/proc') / str(pid) / 'stat'
                            self.assertTrue(not state.exists() or state.read_text().split()[2] == 'Z',
                                            f'owned child {pid} still running')
                finally:
                    groups = {process.pid}
                    groups.update(record['group'] for record in records)
                    for group in groups:
                        try: os.killpg(group, signal.SIGKILL)
                        except ProcessLookupError: pass
                    process.wait(timeout=4)

    def test_stress_cancellation_during_build_stops_build_descendants(self):
        fake = self.work / 'build-tools'
        trace = self.work / 'build-started.json'
        config = self.work / 'build-client.json'; config.write_text('{}')
        self.program(fake / 'cmake', """import json, os, pathlib, signal, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
def stop(*_):
    child.terminate(); child.wait(timeout=2); sys.exit(0)
signal.signal(signal.SIGTERM, stop)
pathlib.Path(os.environ['BUILD_TRACE']).write_text(json.dumps({'pid': os.getpid(), 'group': os.getpgrp(), 'child': child.pid}))
while True: time.sleep(.1)
""")
        env = self.env.copy()
        env.update(PATH=str(fake) + os.pathsep + env['PATH'], BUILD_TRACE=str(trace),
                   SHUTDOWN_TIMEOUT_SECONDS='1', BUILD_JOBS='1')
        process = subprocess.Popen(['bash', str(ROOT / 'stress_test.sh'), '--build',
            '--build-dir', str(self.work / 'build-cancel'), '--client-config', str(config)],
            env=env, cwd=self.work, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)
        record = None
        try:
            deadline = time.monotonic() + 4
            while not trace.exists() and time.monotonic() < deadline:
                self.assertIsNone(process.poll()); time.sleep(.01)
            self.assertTrue(trace.exists())
            record = json.loads(trace.read_text())
            process.send_signal(signal.SIGTERM)
            self.assertEqual(process.wait(timeout=4), 143)
            for pid in (record['pid'], record['child']):
                state = Path('/proc') / str(pid) / 'stat'
                self.assertTrue(not state.exists() or state.read_text().split()[2] == 'Z')
        finally:
            for group in {process.pid, record['group'] if record else process.pid}:
                try: os.killpg(group, signal.SIGKILL)
                except ProcessLookupError: pass
            process.wait(timeout=4)

    def test_stress_preserves_client_failure_status(self):
        build = self.work / 'failure-build'
        config = self.work / 'failure-client.json'; config.write_text('{}')
        self.program(build / 'stress_test_client', 'raise SystemExit(7)\n')
        result = self.run_script('stress_test.sh', ['--build-dir', str(build), '--client-config', str(config)])
        self.assertEqual(result.returncode, 7, result.stderr)

    def test_build_passes_selected_triplet_and_refreshes_only_when_it_changes(self):
        fake = self.work / "tools"
        trace = self.work / "cmake.jsonl"
        self.program(fake / "cmake", """import json, os, pathlib, shutil, sys
args = sys.argv[1:]
with open(os.environ['TRACE'], 'a') as out: out.write(json.dumps(args) + '\\n')
if args[:3] == ['-E', 'remove', '-f']:
    pathlib.Path(args[3]).unlink(missing_ok=True)
elif args[:2] == ['-E', 'remove_directory']:
    shutil.rmtree(args[2], ignore_errors=True)
elif '-B' in args:
    directory = pathlib.Path(args[args.index('-B') + 1]); directory.mkdir(parents=True, exist_ok=True)
    (directory / 'CMakeCache.txt').write_text('cache')
    (directory / 'CMakeFiles').mkdir(exist_ok=True)
""")
        toolchain = self.work / "sdk/scripts/buildsystems/vcpkg.cmake"
        toolchain.parent.mkdir(parents=True); toolchain.write_text("# fixture")
        self.env.update(PATH=str(fake) + os.pathsep + self.env['PATH'], TRACE=str(trace),
                        BUILD_DIR="output dir", BUILD_JOBS="2", VCPKG_ROOT="sdk",
                        VCPKG_DEFAULT_TRIPLET="x64-linux", VCPKG_TARGET_TRIPLET="x64-custom")
        for index, triplet in enumerate(("x64-custom", "x64-custom", "arm64-linux")):
            self.env['VCPKG_TARGET_TRIPLET'] = triplet
            # 同一构建入口同时支持 ./rpc_build.sh 和教程的 sh rpc_build.sh。
            if index == 0:
                result = subprocess.run(['sh', str(ROOT / 'rpc_build.sh')],
                    cwd=self.work, env=self.env, capture_output=True, text=True, timeout=15)
            elif index == 1:
                result = subprocess.run([str(ROOT / 'rpc_build.sh')],
                    cwd=self.work, env=self.env, capture_output=True, text=True, timeout=15)
            else:
                result = self.run_script("rpc_build.sh")
            self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        configurations = [args for args in records if '-B' in args]
        self.assertEqual(len(configurations), 3)
        for args, triplet in zip(configurations, ("x64-custom", "x64-custom", "arm64-linux")):
            self.assertIn('-DVCPKG_TARGET_TRIPLET=' + triplet, args)
            self.assertEqual(args[args.index('-B') + 1], str(self.work / "output dir"))
        self.assertEqual(sum(args[:2] == ['-E', 'remove_directory'] for args in records), 1)
        self.assertEqual(sum(args[:3] == ['-E', 'remove', '-f'] for args in records), 1)
        self.env.pop('VCPKG_TARGET_TRIPLET')
        self.env['VCPKG_DEFAULT_TRIPLET'] = 'x64-fallback'
        result = self.run_script('rpc_build.sh')
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in trace.read_text().splitlines()]
        self.assertIn('-DVCPKG_TARGET_TRIPLET=x64-fallback', [args for args in records if '-B' in args][-1])

if __name__ == '__main__':
    unittest.main()
