"""Build an isolated C-client copy with the upstream ZOOKEEPER-4810 TLS fix.

For older vcpkg source trees only. Does not modify the installed SDK or suppress
TSan reports. Newer fixed dependencies should be used directly instead.
"""
import argparse
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--source', type=Path, required=True, help='vcpkg-patched zookeeper-client-c source directory')
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
output = args.output.resolve()
source = output / 'source'
shutil.copytree(args.source, source, dirs_exist_ok=True)
path = source / 'src/zookeeper.c'
code = path.read_text()
old = '    static char buf[134] = { 0 };'
fixed = '    static __thread char buf[134] = { 0 };'
if old not in code and fixed not in code:
    raise SystemExit('Unknown source version: no matching endpoint buffer; refusing to patch')
path.write_text(code.replace(old, fixed, 1))
subprocess.run(['cmake', '-S', str(source), '-B', str(output / 'build'),
    '-DCMAKE_BUILD_TYPE=Debug', '-DWANT_CPPUNIT=OFF', '-DWANT_SYNCAPI=ON',
    '-DWITH_CYRUS_SASL=OFF', '-DWITH_OPENSSL=OFF',
    '-DCMAKE_INSTALL_PREFIX=' + str(output / 'install')], check=True)
subprocess.run(['cmake', '--build', str(output / 'build'), '--parallel', '2'], check=True)
subprocess.run(['cmake', '--install', str(output / 'build')], check=True)
print('Use -Dunofficial-zookeeper_DIR=' + str(output / 'install/share/unofficial-zookeeper'))
