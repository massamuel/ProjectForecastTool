import tempfile, unittest
from pathlib import Path
import app
from rag.config import Config
from rag.pipeline import Pipeline
from tests.helpers import FakeEmbedder, MemoryStore, FakeLLM

class ForecastTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.old=app.DB; app.DB=Path(self.tmp.name)/'test.sqlite3'; app.init()
        app._PIPELINE=Pipeline(Config(Path(self.tmp.name)/'vectors'),app.connect,app.DB,FakeEmbedder(),MemoryStore(),FakeLLM())
        app._PIPELINE_KEY=str(app.DB.resolve())
        self.pid=app.perform('/api/demo',{})['id']
    def tearDown(self):
        app._PIPELINE=None; app.DB=self.old; self.tmp.cleanup()
    def forecast(self):
        with app.connect() as db: return app.forecast(db,self.pid)
    def test_pivot_requires_approval(self):
        before=self.forecast(); self.assertAlmostEqual(before['final_hours'],230)
        app.perform('/api/approve',{'project_id':self.pid,'id':before['changes'][0]['id'],'approved':True})
        after=self.forecast(); self.assertAlmostEqual(after['final_hours'],285.2); self.assertLess(after['budget_variance'],0)
    def test_removed_scope_does_not_remove_spend(self):
        app.perform('/api/change',{'project_id':self.pid,'description':'Remove remaining work','hours':-1000})
        c=self.forecast()['changes'][0]
        app.perform('/api/approve',{'project_id':self.pid,'id':c['id'],'approved':True})
        self.assertEqual(self.forecast()['final_hours'],92)
    def test_ingestion_and_project_isolation(self):
        r=app.perform('/api/ingest',{'project_id':self.pid,'documents':[{'name':'brief','text':'onboarding '*450}]})
        self.assertEqual(r['chunks'],3)
        results=app.perform('/api/search',{'project_id':self.pid,'query':'onboarding'})['results']; self.assertTrue(results)
        other=app.perform('/api/demo',{})['id']
        with self.assertRaises(ValueError): app.perform('/api/change',{'project_id':other,'description':'Invalid evidence','hours':1,'chunk_id':results[0]['id']})
    def test_invalid_csv_is_atomic(self):
        with self.assertRaises(ValueError): app.perform('/api/time',{'project_id':self.pid,'csv':'task,hours\nValid,10\nInvalid,-2'})
        self.assertEqual(self.forecast()['actual'],92)
    def test_invalid_progress(self):
        with self.assertRaises(ValueError): app.perform('/api/progress',{'project_id':self.pid,'completed':201})
        with self.assertRaises(ValueError): app.perform('/api/projects',{'name':'x','budget':'nan','rate':1,'planned':10})

if __name__=='__main__': unittest.main()
