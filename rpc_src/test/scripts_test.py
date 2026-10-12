"""Exercise script argv/cwd/cache behavior with local fake programs, no SDK download."""
import json
import os
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
                    "VCPKG_ROOT", "VCPKG_TARGET_TRIPLET", "VCPKG_DEFAULT_TRIPLET", "CMAKE_TOOLCHAIN_FILE"):
            self.env.pop(key, None)

    def program(self, path, code):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/usr/bin/env python3\n" + code)
        path.chmod(0o755)

    def run_script(self, name, args=(), env=None):
        return subprocess.run(["bash", str(ROOT / name), *args], cwd=self.work,
                              env=env or self.env, capture_output=True, text=True, timeout=15)

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
        self.assertEqual(record['cwd'], str(build)); self.assertIn(str(client), record['argv'])
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
        for triplet in ("x64-custom", "x64-custom", "arm64-linux"):
            self.env['VCPKG_TARGET_TRIPLET'] = triplet
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
