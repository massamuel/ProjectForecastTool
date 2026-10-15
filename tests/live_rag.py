"""Opt-in real model/Chroma test. Use --with-ollama for real generation too."""
import argparse
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import app
from rag.config import Config
from rag.pipeline import Pipeline


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--with-ollama',action='store_true');args=parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        app.DB=Path(tmp)/'project.sqlite3';app.init()
        config=replace(Config.from_env(app.ROOT),chroma_dir=Path(tmp)/'chroma')
        pipeline=Pipeline(config,app.connect,app.DB)
        first=app.perform('/api/projects',{'name':'Portal','budget':30000,'rate':125,'planned':200,'completed':80})['id']
        other=app.perform('/api/projects',{'name':'Other client','budget':10000,'rate':100,'planned':100})['id']
        docs=[{'name':'client.md','text':'# New scope\nThe client requests a customer-facing portal with customer onboarding. Authentication and data services can be reused.\n# Delivery\nThe agreed delivery deadline is October 30, 2026.'},
              {'name':'infrastructure.md','text':'# Infrastructure\nThe PostgreSQL database must be backed up every night. Infrastructure monitoring alerts go to the operations team.'}]
        result=pipeline.ingest(first,docs);assert result['indexed'],result
        assert pipeline.ingest(first,docs)['duplicates_skipped']==2
        assert pipeline.ingest(other,[{'name':'secret.md','text':'The other client secret is ORANGE-FALCON.'}])['indexed']
        sources=pipeline.retrieve(first,'What is the expected delivery date?')
        assert sources and 'October 30' in sources[0]['text'],sources
        assert all('ORANGE-FALCON' not in s['text'] for s in sources)
        with app.connect() as db:
            ids={r[0] for r in db.execute('SELECT id FROM chunks WHERE project_id=?',(first,))}
        assert all(s['id'] in ids for s in sources)
        assert pipeline.retrieve(first,'What needs nightly database backups?')[0]['source']=='infrastructure.md'
        tokenizer=pipeline.embedder.tokenizer
        with app.connect() as db:
            for r in db.execute('SELECT text FROM chunks'):
                assert len(tokenizer.encode(r['text'],add_special_tokens=False))<=192
        script='import chromadb,sys;from chromadb.config import Settings;c=chromadb.PersistentClient(path=sys.argv[1],settings=Settings(anonymized_telemetry=False));assert sum(x.count() for x in c.list_collections())>=4;print("Persistent Chroma reopened in a second process")'
        subprocess.run([sys.executable,'-c',script,str(config.chroma_dir)],check=True)
        print('PASS: real MiniLM embeddings, semantic top results, project isolation, duplicate uploads, token limits, persisted Chroma')
        if args.with_ollama:
            answer=pipeline.ask(first,'What is the agreed delivery deadline?')
            assert answer['claims'] and not answer['insufficient_evidence'],answer
            assert any('30' in c['text'] for c in answer['claims']),answer
            findings=pipeline.extract(first,'What customer portal scope changes and delivery deadlines are explicitly requested?')
            assert findings['findings'],findings
            with app.connect() as db:assert app.forecast(db,first)['scope_delta']==0
            print('PASS: real Ollama cited answer and exact-quote structured extraction; forecast unchanged')
            print(json.dumps({'answer':answer['claims'],'findings':findings['findings']},indent=2))

if __name__=='__main__':main()
