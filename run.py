"""Build and supervise the local Go API + Python processing service."""
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent

def wait_ready(url, token, children):
    for _ in range(100):
        if any(p.poll() is not None for p in children):
            raise RuntimeError('A service exited during startup')
        try:
            req = urllib.request.Request(url, headers={'X-Internal-Token': token})
            with urllib.request.urlopen(req, timeout=.5) as r:
                if r.status == 200: return
        except (OSError, urllib.error.URLError):
            time.sleep(.1)
    raise RuntimeError('Service readiness timed out')

def main():
    local_go = ROOT / '.tools' / 'go' / 'bin' / 'go'
    go = os.environ.get('GO_BIN') or shutil.which('go') or (str(local_go) if local_go.is_file() else None)
    if not go: raise SystemExit('Go 1.22+ is required. Install from https://go.dev/doc/install or set GO_BIN to its executable.')
    env = os.environ.copy()
    env['FORECAST_INTERNAL_TOKEN'] = secrets.token_urlsafe(32)
    port = int(env.get('PORT', '8765'))
    python_port = int(env.get('PYTHON_PORT', '8766'))
    if not 1 <= port <= 65535 or not 1 <= python_port <= 65535 or port == python_port:
        raise SystemExit('PORT and PYTHON_PORT must be distinct valid ports')
    # Fail before starting either service if an unrelated process owns a port.
    import socket
    for n in (port, python_port):
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try: sock.bind(('127.0.0.1', n))
            except OSError: raise SystemExit('Port %d is already in use' % n)
    env['PYTHON_SERVICE_URL'] = 'http://127.0.0.1:%d' % python_port
    binary = ROOT / '.bin' / 'forecast-backend'
    binary.parent.mkdir(exist_ok=True)
    subprocess.run([go, 'build', '-o', str(binary), './backend'], cwd=ROOT, env=env, check=True)
    children = []
    def stop(_sig, _frame): raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    try:
        # Own each child process group, so Ctrl+C is handled once by this supervisor.
        children.append(subprocess.Popen([sys.executable, 'app.py'], cwd=ROOT, env=env, start_new_session=True))
        wait_ready(env['PYTHON_SERVICE_URL']+'/health', env['FORECAST_INTERNAL_TOKEN'], children)
        children.append(subprocess.Popen([str(binary)], cwd=ROOT, env=env, start_new_session=True))
        wait_ready('http://127.0.0.1:%d/api/health' % port, env['FORECAST_INTERNAL_TOKEN'], children)
        print('ProjectForecast ready: http://127.0.0.1:%d (Ctrl+C stops both services)' % port, flush=True)
        while all(p.poll() is None for p in children): time.sleep(.25)
        raise RuntimeError('A service stopped unexpectedly')
    except KeyboardInterrupt:
        print('\nStopping Go and Python services...', flush=True)
    finally:
        # Stop Go first while Python is still available to finish outstanding requests.
        for p in reversed(children):
            if p.poll() is None:
                p.terminate()
                try: p.wait(timeout=8)
                except subprocess.TimeoutExpired: p.kill(); p.wait()

if __name__ == '__main__':
    try: main()
    except (RuntimeError, subprocess.CalledProcessError) as e: raise SystemExit(str(e))
