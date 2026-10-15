import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import app
from rag.chunking import chunk_document
from rag.config import Config
from rag.generation import InvalidGeneration, Ollama, validate_answer, validate_findings
from rag.pipeline import Pipeline
from .helpers import FakeEmbedder, FakeLLM, MemoryStore, Tokenizer


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.old=app.DB;app.DB=Path(self.tmp.name)/'projects.db';app.init()
        self.pid=app.perform('/api/projects',{'name':'A','budget':10000,'rate':100,'planned':100})['id']
        self.other=app.perform('/api/projects',{'name':'B','budget':10000,'rate':100,'planned':100})['id']
        self.store=MemoryStore();self.llm=FakeLLM()
        self.pipeline=Pipeline(Config(Path(self.tmp.name)/'chroma'),app.connect,app.DB,FakeEmbedder(),self.store,self.llm)
    def tearDown(self):app.DB=self.old;app._PIPELINE=None;self.tmp.cleanup()
    def ingest(self,pid=None):
        return self.pipeline.ingest(pid or self.pid,[{'name':'brief.md','text':'# Pivot\nClient requests a customer portal.\n# Deadline\nDelivery is due October 30.'}])
    def test_sections_token_limits_and_provenance(self):
        text='# First\n'+'word '*500+'\n# Second\nNew article.'
        chunks=chunk_document({'text':text},Tokenizer(),192,32)
        self.assertGreater(len(chunks),3)
        self.assertTrue(all(c['token_count']<=192 and c['text'] in text for c in chunks))
        self.assertFalse(any('# Second' in c['text'] for c in chunks if c['section']=='First'))
        self.assertGreater(chunks[-1]['line_start'],1)
    def test_duplicate_ingest_and_project_filter(self):
        self.assertTrue(self.ingest()['indexed'])
        second=self.ingest();self.assertEqual(second['chunks'],0);self.assertEqual(second['duplicates_skipped'],1)
        self.ingest(self.other)
        sources=self.pipeline.retrieve(self.pid,'project pivot')
        with app.connect() as db:
            own={r[0] for r in db.execute('SELECT id FROM chunks WHERE project_id=?',(self.pid,))}
        self.assertTrue(sources);self.assertTrue(all(s['id'] in own for s in sources))
    def test_index_failure_is_recoverable(self):
        with patch.object(self.store,'replace_project',side_effect=RuntimeError('disk busy')):
            result=self.ingest();self.assertFalse(result['indexed'])
        self.assertTrue(self.pipeline.reindex(self.pid)['indexed'])
        self.assertTrue(self.pipeline.retrieve(self.pid,'pivot'))
    def test_no_evidence_does_not_call_llm(self):
        self.assertTrue(self.pipeline.ask(self.pid,'deadline')['insufficient_evidence'])
        self.assertEqual(self.llm.calls,0)
    def test_ask_citations_and_reject_fabrication(self):
        self.ingest();source=self.pipeline.retrieve(self.pid,'pivot')[0]
        self.llm.value={'claims':[{'text':'A portal is requested.','citations':[source['id']]}],'insufficient_evidence':False}
        self.assertTrue(self.pipeline.ask(self.pid,'pivot')['claims'])
        self.llm.value['claims'][0]['citations']=[99999]
        with self.assertRaises(InvalidGeneration):self.pipeline.ask(self.pid,'pivot')
    def test_exact_quote_and_no_automatic_forecast_change(self):
        self.ingest();source=self.pipeline.retrieve(self.pid,'pivot')[0]
        self.llm.value={'findings':[{'kind':'pivot','description':'Customer portal requested','chunk_id':source['id'],'quote':'Client requests a customer portal.'}]}
        findings=self.pipeline.extract(self.pid,'pivot')['findings'];self.assertEqual(len(findings),1)
        with app.connect() as db:self.assertEqual(app.forecast(db,self.pid)['scope_delta'],0)
        self.llm.value['findings'][0]['quote']='Invented quote'
        with self.assertRaises(InvalidGeneration):self.pipeline.extract(self.pid,'pivot')
    def test_low_similarity_is_not_evidence(self):
        self.ingest()
        with patch.object(self.store,'search',return_value=[{'chunk_id':1,'project_id':self.pid,'text':'x','distance':1.5}]):
            self.assertEqual(self.pipeline.retrieve(self.pid,'unrelated'),[])
    def test_concurrent_duplicate_ingest(self):
        errors=[]
        def ingest():
            try:self.ingest()
            except Exception as e:errors.append(e)
        workers=[threading.Thread(target=ingest) for _ in range(2)]
        for t in workers:t.start()
        for t in workers:t.join()
        self.assertFalse(errors)
        with app.connect() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM rag_documents').fetchone()[0],1)
    def test_old_chunks_reindexed_without_changing_original(self):
        with app.connect() as db:
            db.execute('INSERT INTO chunks(project_id,source,position,text) VALUES(?,?,?,?)',(self.pid,'old',0,'scope '*450))
        self.pipeline.reindex(self.pid)
        self.assertEqual(len(self.store.rows[self.pid]),3)
        with app.connect() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM chunks').fetchone()[0],1)


class GenerationValidationTests(unittest.TestCase):
    def test_boolean_is_not_valid_citation_id(self):
        with self.assertRaises(InvalidGeneration):validate_answer({'claims':[{'text':'x','citations':[True]}],'insufficient_evidence':False},[{'id':1}])
    def test_empty_claims_derive_insufficient_flag(self):
        self.assertTrue(validate_answer({'claims':[]},[])['insufficient_evidence'])
    def test_remote_ollama_rejected(self):
        with self.assertRaises(ValueError):Ollama(Config(Path('/tmp'),ollama_url='https://remote.example'))
    def test_model_unavailable_report(self):
        ollama=Ollama(Config(Path('/tmp')))
        with patch.object(ollama.opener,'open',side_effect=OSError('offline')):
            self.assertFalse(ollama.available()['reachable'])

if __name__=='__main__':unittest.main()
