"""Exercise the actual Go/Python boundary with an isolated SQLite database."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT=Path(__file__).resolve().parents[1]

def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0)); return sock.getsockname()[1]

def main():
    with tempfile.TemporaryDirectory() as tmp:
        port,internal=free_port(),free_port()
        while internal==port: internal=free_port()
        env=dict(os.environ, PORT=str(port), PYTHON_PORT=str(internal), FORECAST_DB=str(Path(tmp)/'test.sqlite3'))
        base='http://127.0.0.1:'+str(port)
        log_path=Path(tmp)/'server.log'
        def call(path,data=None,method=None):
            req=urllib.request.Request(base+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'},method=method)
            with urllib.request.urlopen(req,timeout=5) as r: return r.status,json.load(r)
        def start():
            log=open(log_path,'ab')
            p=subprocess.Popen([sys.executable,'run.py'],cwd=ROOT,env=env,stdout=log,stderr=log,start_new_session=True);log.close()
            for _ in range(300):
                if p.poll() is not None: raise AssertionError(log_path.read_text())
                try:
                    if call('/api/health')[1]['backend']=='go':return p
                except (OSError,urllib.error.URLError):pass
                time.sleep(.1)
            p.terminate();p.wait(timeout=10);raise AssertionError('startup timed out\n'+log_path.read_text())
        def stop(p):
            p.send_signal(signal.SIGTERM)
            try:p.wait(timeout=12)
            except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait();raise AssertionError('shutdown timed out')
            assert p.returncode==0,log_path.read_text()
            for n in (port,internal):
                with socket.socket() as sock:
                    sock.settimeout(.5);assert sock.connect_ex(('127.0.0.1',n))!=0,'Service left running'
        p=start()
        try:
            pid=call('/api/projects',{'name':'Integration','budget':30000,'rate':125,'planned':200,'completed':80})[1]['id']
            call('/api/time',{'project_id':pid,'csv':'task,hours\nDelivery,92'})
            status,j=call('/api/ingest',{'project_id':pid,'documents':[{'name':'client.md','text':'Client requested onboarding and a portal pivot. '*100}]})
            assert status==202
            for _ in range(100):
                _,j=call('/api/jobs/'+j['id'])
                if j['status'] not in ('queued','running'):break
                time.sleep(.05)
            assert j['status']=='succeeded',j
            assert j['result']['chunks']>1
            result=call('/api/search',{'project_id':pid,'query':'portal pivot'})[1]['results'][0]
            call('/api/change',{'project_id':pid,'description':'Portal pivot','hours':48,'chunk_id':result['id']})
            project=call('/api/state')[1]['projects'][0]
            assert project['final_cost']==28750,project
            call('/api/approve',{'project_id':pid,'id':project['changes'][0]['id'],'approved':True})
            project=call('/api/state')[1]['projects'][0]
            assert abs(project['final_cost']-35650)<.01,project
            try:
                urllib.request.urlopen('http://127.0.0.1:%d/api/state'%internal)
                raise AssertionError('Internal API accepted missing token')
            except urllib.error.HTTPError as e:assert e.code==403
            print('PASS: Go health, create project, time import, async upload, retrieval, scope approval, internal token')
        finally:stop(p)
        p=start()
        try:
            project=call('/api/state')[1]['projects'][0]
            assert abs(project['final_cost']-35650)<.01
            assert call('/api/search',{'project_id':pid,'query':'onboarding'})[1]['results']
            print('PASS: SQLite data survives service restart')
        finally:stop(p)
        print('PASS: supervisor shuts down both services cleanly')

if __name__=='__main__':main()
