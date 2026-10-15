"""Ollama JSON-schema generation with locally checked citations and excerpts."""
import json
import threading
import urllib.error
import urllib.request
from urllib.parse import urlparse
from .embeddings import Unavailable


class InvalidGeneration(RuntimeError):
    pass


ANSWER_SCHEMA = {'type':'object','additionalProperties':False,
 'properties':{'claims':{'type':'array','maxItems':8,
 'items':{'type':'object','additionalProperties':False,'properties':{
 'text':{'type':'string'},'citations':{'type':'array','minItems':1,'items':{'type':'integer'}}},
 'required':['text','citations']}}},'required':['claims']}

EXTRACTION_SCHEMA = {'type':'object','additionalProperties':False,'properties':{'findings':{
 'type':'array','maxItems':12,'items':{'type':'object','additionalProperties':False,'properties':{
 'kind':{'type':'string','enum':['requirement','deadline','dependency','risk','scope_change','pivot']},
 'description':{'type':'string'},'chunk_id':{'type':'integer'},'quote':{'type':'string'}},
 'required':['kind','description','chunk_id','quote']}}},'required':['findings']}


class Ollama:
    def __init__(self,config):
        u=urlparse(config.ollama_url)
        if u.scheme!='http' or u.hostname not in ('localhost','127.0.0.1','::1') or u.path not in ('','/') or u.query or u.username:
            raise ValueError('OLLAMA_URL must be a loopback HTTP origin')
        self.url=config.ollama_url.rstrip('/');self.model=config.ollama_model
        self.slots=threading.BoundedSemaphore(1)
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def available(self):
        try:
            with self.opener.open(self.url+'/api/tags',timeout=2) as r:tags=json.load(r)
            models=[m.get('name') for m in tags.get('models',[])]
            return {'reachable':True,'model':self.model,'model_installed':self.model in models}
        except (OSError,ValueError):return {'reachable':False,'model':self.model,'model_installed':False}

    def generate(self,system,prompt,schema):
        if not self.slots.acquire(blocking=False):
            raise Unavailable('A local model request is already running. Please try again shortly.')
        try:
            body=json.dumps({'model':self.model,'stream':False,'format':schema,
                'options':{'temperature':0,'num_ctx':4096,'num_predict':1200},
                'messages':[{'role':'system','content':system},{'role':'user','content':prompt}]}).encode()
            req=urllib.request.Request(self.url+'/api/chat',data=body,headers={'Content-Type':'application/json'})
            try:
                with self.opener.open(req,timeout=180) as r:
                    response=json.loads(r.read(1_000_001))
                if not response.get('done') or response.get('done_reason')=='length':
                    raise InvalidGeneration('Model response was incomplete. Retry with a narrower question.')
                return json.loads(response['message']['content'])
            except urllib.error.HTTPError as e:
                raise Unavailable('Ollama rejected the request. Start Ollama and pull '+self.model+'.') from e
            except (OSError,KeyError,ValueError) as e:
                raise Unavailable('Ollama is unavailable or returned invalid JSON. Start Ollama and pull '+self.model+'.') from e
        finally:self.slots.release()


def validate_answer(value,sources):
    allowed={s['id'] for s in sources}
    if not isinstance(value,dict) or not isinstance(value.get('claims'),list):
        raise InvalidGeneration('Model did not return the expected answer structure')
    claims=value['claims']
    if len(claims)>8:
        raise InvalidGeneration('Model returned too many claims')
    for c in claims:
        if not isinstance(c,dict) or not isinstance(c.get('text'),str) or not c['text'].strip() or len(c['text'])>2000:
            raise InvalidGeneration('Model returned an invalid claim')
        ids=c.get('citations')
        if not isinstance(ids,list) or not ids or any(type(i) is not int or i not in allowed for i in ids):
            raise InvalidGeneration('Model cited a source outside the retrieved project evidence')
    return {'claims':claims,'insufficient_evidence':not bool(claims)}


def validate_findings(value,sources):
    by_id={s['id']:s for s in sources};kinds=set(EXTRACTION_SCHEMA['properties']['findings']['items']['properties']['kind']['enum'])
    if not isinstance(value,dict) or not isinstance(value.get('findings'),list) or len(value['findings'])>12:
        raise InvalidGeneration('Model did not return the expected extraction structure')
    result=[];seen=set()
    for f in value['findings']:
        if not isinstance(f,dict) or not isinstance(f.get('kind'),str) or f.get('kind') not in kinds or type(f.get('chunk_id')) is not int or f['chunk_id'] not in by_id:
            raise InvalidGeneration('Extracted finding has an invalid type or source')
        desc=f.get('description');quote=f.get('quote')
        if not isinstance(desc,str) or not desc.strip() or len(desc)>1000 or not isinstance(quote,str) or not quote.strip() or quote not in by_id[f['chunk_id']]['text']:
            raise InvalidGeneration('Extracted evidence must quote the source exactly')
        key=(f['kind'],f['chunk_id'],quote)
        if key not in seen:result.append(f);seen.add(key)
    return result
