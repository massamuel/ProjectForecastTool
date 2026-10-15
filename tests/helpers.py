"""Small deterministic doubles: unit tests never download models."""
import re
from types import SimpleNamespace


class Tokenizer:
    def __call__(self,text,**kwargs):
        return {'offset_mapping':[(m.start(),m.end()) for m in re.finditer(r'\S+',text)]}
    def encode(self,text,**kwargs):return text.split()
    def num_special_tokens_to_add(self,pair):return 2


class FakeEmbedder:
    tokenizer=Tokenizer()
    max_tokens=256
    def encode(self,texts):return [[1.,0.] for _ in texts]
    def chunks(self,document,size,overlap):
        from rag.chunking import chunk_document
        return chunk_document(document,self.tokenizer,size,overlap)


class MemoryStore:
    def __init__(self):self.collection=SimpleNamespace(name='test-collection');self.rows={}
    def replace_project(self,pid,records,embeddings):self.rows[pid]=[dict(r,project_id=pid) for r in records]
    def search(self,pid,vector,limit):
        return [dict(r,chunk_id=r['id'],distance=.1) for r in self.rows.get(pid,[])][:limit]


class FakeLLM:
    def __init__(self):self.value=None;self.calls=0
    def available(self):return {'reachable':True,'model':'test','model_installed':True}
    def generate(self,*args):self.calls+=1;return self.value
