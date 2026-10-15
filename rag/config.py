"""Local RAG configuration; no hosted credentials required."""
from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Config:
    chroma_dir: Path
    embedding_model: str = 'sentence-transformers/all-MiniLM-L6-v2'
    ollama_url: str = 'http://127.0.0.1:11434'
    ollama_model: str = 'qwen2.5:1.5b'
    chunk_tokens: int = 192
    overlap_tokens: int = 32
    top_k: int = 5
    max_distance: float = 0.75

    @classmethod
    def from_env(cls, root):
        return cls(Path(os.environ.get('CHROMA_DIR', str(root / '.rag' / 'chroma'))),
                   os.environ.get('EMBEDDING_MODEL', cls.embedding_model),
                   os.environ.get('OLLAMA_URL', cls.ollama_url),
                   os.environ.get('OLLAMA_MODEL', cls.ollama_model))
