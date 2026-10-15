"""One lazy, CPU-friendly Sentence Transformer shared by request threads."""
import os
from pathlib import Path
import threading


class Unavailable(RuntimeError):
    pass


class Embedder:
    def __init__(self, model_name):
        self.model_name = model_name
        self._model = None
        self._lock = threading.RLock()

    def _load(self):
        if self._model is None:
            try:
                cache = os.environ.get('RAG_MODEL_CACHE', str(Path(__file__).resolve().parents[1]/'.rag'/'models'))
                os.environ.setdefault('HF_HOME', str(Path(cache).parent/'huggingface'))
                os.environ.setdefault('TRANSFORMERS_CACHE', cache)
                os.environ.setdefault('HF_HUB_DISABLE_TELEMETRY', '1')
                os.environ.setdefault('TOKENIZERS_PARALLELISM', 'false')
                from sentence_transformers import SentenceTransformer
                # Only model data is loaded; never execute repository-supplied code.
                self._model = SentenceTransformer(self.model_name, device='cpu',
                    cache_folder=cache,
                    trust_remote_code=False, model_kwargs={'use_safetensors': True})
            except Exception as e:
                raise Unavailable('Embedding model unavailable. Install requirements.txt and run python -m rag.setup. ' + str(e)) from e
        return self._model

    @property
    def tokenizer(self):
        with self._lock:
            return self._load().tokenizer

    @property
    def max_tokens(self):
        with self._lock:
            return self._load().max_seq_length

    def encode(self, texts):
        with self._lock:
            model=self._load()
            if any(len(model.tokenizer.encode(text, add_special_tokens=True))>model.max_seq_length for text in texts):
                raise ValueError('Text exceeds embedding token capacity; shorten the question or rebuild tokenizer-aware chunks')
            # Batch rather than launching competing inference threads on one CPU.
            return model.encode(texts, batch_size=32, normalize_embeddings=True,
                                       show_progress_bar=False).tolist()

    def chunks(self, document, size, overlap):
        from .chunking import chunk_document
        with self._lock:
            return chunk_document(document, self._load().tokenizer, size, overlap)
