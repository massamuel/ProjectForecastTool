import csv, io, json, math, os, re, sqlite3, threading
from contextlib import contextmanager
from rag.config import Config
from rag.pipeline import Pipeline
from rag.embeddings import Unavailable
from rag.generation import InvalidGeneration
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).parent
DB = Path(os.environ.get('FORECAST_DB', str(ROOT / 'forecast.sqlite3')))

@contextmanager
def connect():
    db = sqlite3.connect(DB, timeout=20)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA foreign_keys=ON')
    try:
        with db:
            yield db
    finally:
        db.close()

def init():
    with connect() as db:
        db.executescript('''CREATE TABLE IF NOT EXISTS projects(id INTEGER PRIMARY KEY, name TEXT NOT NULL, budget REAL NOT NULL, rate REAL NOT NULL, planned REAL NOT NULL, completed REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS entries(id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id), task TEXT, hours REAL);
        CREATE TABLE IF NOT EXISTS chunks(id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id), source TEXT, position INTEGER, text TEXT);
        CREATE TABLE IF NOT EXISTS changes(id INTEGER PRIMARY KEY, project_id INTEGER REFERENCES projects(id), description TEXT, hours REAL, approved INTEGER DEFAULT 0, chunk_id INTEGER REFERENCES chunks(id));''')

        # Additive migration: existing project/time/scope data remains usable.
        existing = {r[1] for r in db.execute('PRAGMA table_info(chunks)')}
        columns = {'document_id':"TEXT NOT NULL DEFAULT ''", 'section':"TEXT NOT NULL DEFAULT 'Document'",
                   'line_start':'INTEGER NOT NULL DEFAULT 1','line_end':'INTEGER NOT NULL DEFAULT 1',
                   'token_count':'INTEGER NOT NULL DEFAULT 0'}
        for name, definition in columns.items():
            if name not in existing: db.execute('ALTER TABLE chunks ADD COLUMN '+name+' '+definition)
        db.executescript('''CREATE TABLE IF NOT EXISTS rag_documents(id TEXT PRIMARY KEY, project_id INTEGER REFERENCES projects(id), name TEXT NOT NULL, text TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS rag_index_state(project_id INTEGER PRIMARY KEY REFERENCES projects(id), signature TEXT NOT NULL, collection TEXT NOT NULL);''')

_PIPELINE = None
_PIPELINE_KEY = None
_PIPELINE_LOCK = threading.Lock()

def get_pipeline():
    global _PIPELINE, _PIPELINE_KEY
    key = str(DB.resolve())
    with _PIPELINE_LOCK:
        if _PIPELINE is None or _PIPELINE_KEY != key:
            _PIPELINE = Pipeline(Config.from_env(ROOT), connect, DB)
            _PIPELINE_KEY = key
        return _PIPELINE

def number(data, key, default=0, positive=False):
    value = float(data.get(key, default))
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(key + ' must be a finite ' + ('positive' if positive else 'nonnegative') + ' number')
    return value

def forecast(db, pid):
    p = db.execute('SELECT * FROM projects WHERE id=?', (pid,)).fetchone()
    if not p: raise ValueError('Project not found')
    actual = db.execute('SELECT COALESCE(SUM(hours),0) FROM entries WHERE project_id=?', (pid,)).fetchone()[0]
    changes = [dict(r) for r in db.execute('SELECT c.*, k.source, k.text AS evidence FROM changes c LEFT JOIN chunks k ON k.id=c.chunk_id WHERE c.project_id=? ORDER BY c.id DESC', (pid,))]
    delta = sum(c['hours'] for c in changes if c['approved'])
    # completed is accepted baseline work, not percentage of a changing scope.
    baseline_remaining = max(0, p['planned'] - p['completed'])
    factor = max(0.5, min(3, actual / p['completed'])) if p['completed'] > 0 and actual > 0 else 1
    remaining = max(0, (baseline_remaining + delta) * factor)
    final = actual + remaining
    cost = final * p['rate']
    return dict(p, actual=actual, scope_delta=delta, pace_factor=factor, remaining=remaining, final_hours=final, final_cost=cost, budget_variance=p['budget']-cost, low_cost=(actual+remaining*.8)*p['rate'], high_cost=(actual+remaining*1.3)*p['rate'], changes=changes, documents=db.execute('SELECT COUNT(DISTINCT source) FROM chunks WHERE project_id=?',(pid,)).fetchone()[0])

def perform(path, data):
    if path == '/api/ingest':
        return get_pipeline().ingest(int(data['project_id']), data['documents'])
    if path in ('/api/reindex','/api/ask','/api/extract'):
        pipeline=get_pipeline(); pid=int(data['project_id'])
        if path == '/api/reindex': return pipeline.reindex(pid)
        question=data.get('query','')
        return pipeline.ask(pid,question) if path=='/api/ask' else pipeline.extract(pid,question)
    with connect() as db:
        if path == '/api/projects':
            planned = number(data,'planned',positive=True)
            completed = number(data,'completed')
            if completed > planned: raise ValueError('Completed baseline hours cannot exceed planned hours')
            name = str(data.get('name','')).strip()[:200]
            if not name: raise ValueError('Project name is required')
            cur = db.execute('INSERT INTO projects(name,budget,rate,planned,completed) VALUES(?,?,?,?,?)',(name,number(data,'budget',positive=True),number(data,'rate',positive=True),planned,completed))
            return {'id':cur.lastrowid}
        if path == '/api/demo':
            cur = db.execute('INSERT INTO projects(name,budget,rate,planned,completed) VALUES(?,?,?,?,?)',('Northstar · Client portal',30000,125,200,80))
            pid=cur.lastrowid
            db.execute('INSERT INTO entries(project_id,task,hours) VALUES(?,?,?)',(pid,'Discovery, authentication and dashboard',92))
            cur=db.execute('INSERT INTO chunks(project_id,source,position,text) VALUES(?,?,?,?)',(pid,'Client call · October 12',0,'Client requested a pivot from an internal dashboard to a customer-facing portal. Reuse authentication and data services. Add customer permissions, onboarding and accessibility review. Proposed additional effort: 48 hours, pending project manager approval.'))
            db.execute('INSERT INTO changes(project_id,description,hours,chunk_id) VALUES(?,?,?,?)',(pid,'Pivot to customer-facing portal',48,cur.lastrowid))
            return {'id':pid}
        pid=int(data['project_id'])
        if not db.execute('SELECT id FROM projects WHERE id=?',(pid,)).fetchone(): raise ValueError('Project not found')
        if path == '/api/time':
            rows=list(csv.DictReader(io.StringIO(data['csv'])))
            if not rows or not {'task','hours'}.issubset(rows[0]): raise ValueError('CSV needs task,hours columns and at least one row')
            validated=[(pid,r['task'][:300],number(r,'hours')) for r in rows]
            db.executemany('INSERT INTO entries(project_id,task,hours) VALUES(?,?,?)',validated)
            return {'imported':len(rows)}
        if path == '/api/progress':
            completed=number(data,'completed')
            planned=db.execute('SELECT planned FROM projects WHERE id=?',(pid,)).fetchone()[0]
            if completed>planned: raise ValueError('Completed baseline hours cannot exceed planned hours')
            db.execute('UPDATE projects SET completed=? WHERE id=?',(completed,pid))
            return {'ok':True}
        if path == '/api/search':
            return {'results':get_pipeline().retrieve(pid,data.get('query',''))}
        if path == '/api/change':
            desc=str(data.get('description','')).strip()[:500]
            if not desc: raise ValueError('Describe the scope change')
            hours=float(data['hours'])
            if not math.isfinite(hours): raise ValueError('Hours must be finite')
            cid=data.get('chunk_id') or None
            if cid and not db.execute('SELECT id FROM chunks WHERE id=? AND project_id=?',(cid,pid)).fetchone(): raise ValueError('Evidence belongs to another project')
            db.execute('INSERT INTO changes(project_id,description,hours,chunk_id) VALUES(?,?,?,?)',(pid,desc,hours,cid))
            return {'ok':True}
        if path == '/api/approve':
            cur=db.execute('UPDATE changes SET approved=? WHERE id=? AND project_id=?',(int(bool(data['approved'])),int(data['id']),pid))
            if not cur.rowcount: raise ValueError('Change not found')
            return {'ok':True}
        raise ValueError('Unknown operation')

class Handler(BaseHTTPRequestHandler):
    def reply(self,value,status=200):
        body=json.dumps(value).encode(); self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        if self.path=='/api/rag/status': return self.reply(get_pipeline().status())
        if self.path=='/':
            body=(ROOT/'index.html').read_bytes(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.end_headers(); self.wfile.write(body); return
        with connect() as db:
            if self.path=='/api/state': return self.reply({'projects':[forecast(db,r[0]) for r in db.execute('SELECT id FROM projects ORDER BY id DESC')]})
        self.reply({'error':'Not found'},404)
    def do_POST(self):
        try:
            # Local app: reject cross-origin browser writes.
            origin=self.headers.get('Origin')
            if origin and origin != 'http://'+self.headers.get('Host',''): return self.reply({'error':'Origin rejected'},403)
            size=int(self.headers.get('Content-Length',0))
            if not 0<size<=2_000_000: raise ValueError('Request must be under 2 MB')
            self.reply(perform(self.path,json.loads(self.rfile.read(size))))
        except Unavailable as e: self.reply({'error':str(e)},503)
        except InvalidGeneration as e: self.reply({'error':str(e)},502)
        except (ValueError,KeyError,TypeError,csv.Error) as e: self.reply({'error':str(e)},400)
        except Exception: self.reply({'error':'Internal error; check server console'},500); import traceback; traceback.print_exc()

if __name__=='__main__':
    init(); port=int(os.environ.get('PORT','8765')); print('ProjectForecast running at http://127.0.0.1:'+str(port),flush=True)
    ThreadingHTTPServer(('127.0.0.1',port),Handler).serve_forever()
