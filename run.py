"""Start the local Python RAG app and, if available, a local Ollama runtime."""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from urllib.parse import urlparse

ROOT=Path(__file__).resolve().parent


def reachable(url):
    try:
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(url+'/api/tags',timeout=1):return True
    except OSError:return False


def main():
    python=ROOT/'.venv'/'bin'/'python'
    if not python.is_file():python=Path(sys.executable)
    env=os.environ.copy()
    url=env.get('OLLAMA_URL','http://127.0.0.1:11434').rstrip('/')
    parsed=urlparse(url)
    if parsed.scheme!='http' or parsed.hostname not in ('localhost','127.0.0.1','::1') or parsed.path or parsed.query or parsed.username:
        raise SystemExit('OLLAMA_URL must be a loopback HTTP origin')
    local=ROOT/'.tools'/'ollama'/'ollama'
    ollama=env.get('OLLAMA_BIN') or shutil.which('ollama') or (str(local) if local.is_file() else None)
    children=[]
    def stop(*_):raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    try:
        if not reachable(url) and ollama:
            ollama_env=dict(env,OLLAMA_HOST=parsed.netloc,OLLAMA_MODELS=str(ROOT/'.rag'/'ollama-models'),OLLAMA_NO_CLOUD='1')
            children.append(subprocess.Popen([ollama,'serve'],env=ollama_env,cwd=ROOT,start_new_session=True))
            print('Starting local Ollama; waiting for readiness...',flush=True)
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                if children[0].poll() is not None:raise RuntimeError('Ollama failed to start')
                if reachable(url):break
                time.sleep(.25)
        if not reachable(url):print('Ollama not running: semantic search can work, but generation needs Ollama.',flush=True)
        children.append(subprocess.Popen([str(python),'app.py'],env=env,cwd=ROOT,start_new_session=True))
        while children[-1].poll() is None:
            if len(children)>1 and children[0].poll() is not None:raise RuntimeError('Owned Ollama process stopped unexpectedly')
            time.sleep(.25)
        if children[-1].returncode:raise RuntimeError('Application failed to start or stopped unexpectedly')
    except KeyboardInterrupt:print('\nStopping owned RAG services...',flush=True)
    finally:
        for child in reversed(children):
            if child.poll() is None:
                child.terminate()
                try:child.wait(timeout=8)
                except subprocess.TimeoutExpired:child.kill();child.wait()

if __name__=='__main__':
    try:main()
    except RuntimeError as e:raise SystemExit(str(e))
