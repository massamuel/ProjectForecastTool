# ProjectForecastTool — local RAG pipeline

This branch implements **Chroma PersistentClient + Sentence Transformers MiniLM + Ollama**, coordinated through explicit Python modules. The original rules-based labor forecasting remains unchanged. Go coordination is on the separate `go-backend` branch and is not merged here.

## Setup

Use Python **3.10–3.12** (3.12 tested). On Intel macOS, requirements pin PyTorch 2.2.2, the last available Intel Mac wheel, with a compatible NumPy/tokenizer stack. The model loads with safetensors and remote-code execution disabled.

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m rag.setup
```

`rag.setup` downloads `sentence-transformers/all-MiniLM-L6-v2` into `.rag/models` and reports Ollama/model availability. Initial model/package downloads require Internet access; once cached, inference and document processing are local. Allow disk space for Python dependencies and model weights.

Install [Ollama](https://ollama.com/download), then:

```sh
ollama serve
# In another terminal:
ollama pull qwen2.5:1.5b
```

Start the app:

```sh
python3 run.py
```

Open http://127.0.0.1:8765. The launcher uses `.venv/bin/python` when present and starts Ollama if it is available on PATH or at `.tools/ollama/ollama`. It reuses an already-running Ollama server. Ctrl+C stops only services this launcher owns. It never downloads Ollama models automatically. Missing Ollama disables generation, while semantic search can still work. Missing embedding dependencies/model produce an explicit error, not keyword answers presented as RAG.

To run only the app against an existing Ollama server: `.venv/bin/python app.py`.

## Try it

1. Load the fictional sample or create a project.
2. Upload `.txt`, `.md`, or `.json` project documents. One file can contain several Markdown sections/articles.
3. Use **Semantic evidence search** to retrieve relevant passages. Existing MVP data is automatically indexed the first time it is searched.
4. Under **Ask your project evidence**, generate an answer with source/chunk citations, or extract requirements, deadlines, dependencies, risks, scope changes, and pivots.
5. Expand sources to inspect their exact text and line locations. For extracted pivots/scope changes, **Review as scope proposal** pre-fills the description and evidence ID. You must supply an effort estimate, submit the proposal, and approve it before the forecast changes.
6. Use **Rebuild index** to recover an indexing failure or rebuild derived vectors. It preserves stored source evidence and scope references.

## Architecture and review map

| File | Responsibility |
| --- | --- |
| `rag/chunking.py` | Heading/article-aware tokenizer windows with exact text excerpts, section labels, line locations, 192-token target and 32-token overlap |
| `rag/embeddings.py` | Lazy shared MiniLM, 384-dimensional normalized vectors, batches of 32; locks protect model/tokenizer access |
| `rag/store.py` | Chroma PersistentClient, cosine index, project metadata filters, deterministic vector upserts, stale-vector cleanup |
| `rag/generation.py` | Local Ollama HTTP client, bounded generation, JSON schemas, citation-ID and exact-quote validation |
| `rag/pipeline.py` | Ingestion, duplicate detection, recoverable index synchronization, retrieval, cited answers, structured extraction |
| `rag/config.py` | Model, storage, retrieval, and local endpoint configuration |
| `app.py` | HTTP API, additive SQLite migrations, scope approvals, unchanged forecasting baseline |
| `run.py` | Optional local service supervision |
| `tests/test_rag.py` | Offline behavioral tests with deterministic doubles |
| `tests/live_rag.py` | Opt-in real embedding/vector-store/generation smoke tests |

SQLite is the source of truth for documents, original chunk IDs, time entries, and scope references. Chroma is derived and can be rebuilt. Collection namespaces include the SQLite database path and embedding model to avoid mixing databases or incompatible embeddings. Every vector query filters by project; returned IDs/text are also checked against that project's SQLite snapshot.

Ingestion is bounded to two active uploads, with up to four preparation workers per upload. Shared tokenizer access and local embedding inference are serialized; embeddings are batched rather than launching competing model instances. Ollama permits one active generation call. New uploads have at most 20 documents and a 2 MB request limit. Identical project/name/text uploads are skipped. Changed documents are retained as separate evidence versions; no implicit deletion or claim that a newer date supersedes an older one.

SQLite and Chroma cannot share one atomic transaction. Documents commit first; failed indexing returns `indexed: false` and a warning. Search detects changed snapshots and retries indexing. A crash or manual deletion of the Chroma directory may require **Rebuild index**. Existing word-based chunks remain intact for old scope references; vector indexing splits them into token-sized excerpts to prevent embedding truncation.

The LLM sees a bounded top-five evidence set, not the entire document. Retrieval uses a configurable-in-code cosine-distance cutoff of 0.75, an MVP heuristic rather than a calibrated relevance guarantee. Missing evidence returns an insufficient-evidence result. Instructions inside documents are treated as untrusted data. Citation validation checks that IDs belong to retrieved sources; extraction validates exact quotes. These checks do **not** prove that every generated claim is factually entailed, so users must review answers and conflicting requirements. Extraction is focused on retrieved passages, not exhaustive extraction of every document fact.

## API additions

- `GET /api/rag/status`: dependency and Ollama/model readiness.
- `POST /api/ingest`: `{project_id, documents:[{name,text}]}`; returns chunks, duplicates skipped, indexing status, and warning on recoverable indexing failure.
- `POST /api/search`: `{project_id, query}`; semantic results with chunk IDs, sources, section, line locations, cosine distance.
- `POST /api/ask`: `{project_id, query}`; structured claims with citations, sources, insufficient-evidence flag.
- `POST /api/extract`: `{project_id, query}`; unapproved findings with kind, description, source chunk ID, exact quote.
- `POST /api/reindex`: `{project_id}`; rebuild a project's derived index.

Other project, time-entry, scope-approval, and forecast contracts remain the same. Dependency/model unavailability returns HTTP 503; invalid generated structure or citations returns 502. The UI displays those errors rather than pretending generation succeeded.

## Configuration

| Variable | Default |
| --- | --- |
| `PORT` | `8765` |
| `FORECAST_DB` | `forecast.sqlite3` beside app.py |
| `CHROMA_DIR` | `.rag/chroma` |
| `RAG_MODEL_CACHE` | `.rag/models` |
| `EMBEDDING_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` |
| `OLLAMA_URL` | `http://127.0.0.1:11434` (loopback origins only) |
| `OLLAMA_MODEL` | `qwen2.5:1.5b` |
| `OLLAMA_BIN` | Optional explicit executable path for launcher |

Models, virtual environments, Chroma data, SQLite data, and downloaded runtimes are Git-ignored. This is a single-user loopback app, not an authenticated multi-tenant deployment.

## Tests

```sh
# Offline: no packages, downloads, or LLM needed
python3 -m unittest -v

# Real Chroma + MiniLM; uses a temporary project database
.venv/bin/python tests/live_rag.py

# Also exercise real local Ollama answers and structured extraction
.venv/bin/python tests/live_rag.py --with-ollama
```

The live test includes duplicate handling, token limits, a few semantic retrieval examples, cross-project isolation, and reopening persisted Chroma in a second process. It is a smoke test, not a representative retrieval-quality benchmark or a forecast accuracy evaluation.

## Forecasting

Completed baseline hours represent original estimates for accepted work. Remaining effort = max(0, (original planned hours − completed baseline hours + approved remaining scope deltas) × observed effort ratio), with remaining baseline work clamped to zero first. Ratio = actual hours / completed baseline hours, bounded to 0.5–3 and defaulting to 1 without useful observations. Final labor cost = (actual + remaining hours) × blended labor cost rate.

The displayed −20% / +30% range is scenario sensitivity, not a statistical confidence interval. No trained ML, success probabilities, staffing/calendar constraints, or automatic forecast changes are introduced. Those require historical outcomes and separate evaluation.
