"""Chroma is a rebuildable index; SQLite remains the source of truth."""
import hashlib
from .embeddings import Unavailable


class VectorStore:
    def __init__(self, config, namespace):
        try:
            import chromadb
            from chromadb.config import Settings
            self.client = chromadb.PersistentClient(path=str(config.chroma_dir),
                settings=Settings(anonymized_telemetry=False))
            tag = hashlib.sha256((namespace+'|'+config.embedding_model+'|token-v1').encode()).hexdigest()[:24]
            self.collection = self.client.get_or_create_collection('project-'+tag,
                metadata={'hnsw:space':'cosine'}, embedding_function=None)
        except Exception as e:
            raise Unavailable('Chroma unavailable. Install requirements.txt. '+str(e)) from e

    def replace_project(self, pid, records, embeddings):
        # Upsert deterministic IDs; repeated indexing does not duplicate vectors.
        keep = set()
        for start in range(0,len(records),128):
            batch=records[start:start+128]
            ids=[r['vector_id'] for r in batch];keep.update(ids)
            self.collection.upsert(ids=ids,embeddings=embeddings[start:start+128],
                documents=[r['text'] for r in batch], metadatas=[{
                    'project_id':pid,'chunk_id':r['id'],'source':r['source'],
                    'section':r['section'],'line_start':r['line_start'],'line_end':r['line_end']
                } for r in batch])
        previous=self.collection.get(where={'project_id':pid},include=[])['ids']
        stale=[i for i in previous if i not in keep]
        for start in range(0,len(stale),128):self.collection.delete(ids=stale[start:start+128])

    def search(self, pid, vector, limit):
        n=len(self.collection.get(where={'project_id':pid},include=[])['ids'])
        if not n:return []
        r=self.collection.query(query_embeddings=[vector],where={'project_id':pid},
            n_results=min(limit,n),include=['documents','metadatas','distances'])
        return [dict(meta,vector_id=vid,text=text,distance=distance)
                for vid,text,meta,distance in zip(r['ids'][0],r['documents'][0],r['metadatas'][0],r['distances'][0])]
