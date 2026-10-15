"""Download/cache the embedding model explicitly, and report Ollama readiness."""
import json
from pathlib import Path
from .config import Config
from .embeddings import Embedder
from .generation import Ollama

if __name__=='__main__':
    config=Config.from_env(Path(__file__).resolve().parents[1])
    embedder=Embedder(config.embedding_model)
    vector=embedder.encode(['Project requirements and scope changes'])[0]
    print('Embedding model ready: %s (%d dimensions)' % (config.embedding_model,len(vector)))
    print(json.dumps(Ollama(config).available(),indent=2))
    print('If Ollama is not ready: start ollama serve, then ollama pull '+config.ollama_model)
