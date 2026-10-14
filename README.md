# ProjectForecastTool — Go backend

A local project effort/cost forecasting prototype. This branch adds a **Go HTTP API and bounded ingestion queue**, retaining Python for document processing, retrieval, forecasting, and SQLite persistence. It starts from the original MVP on `main`.

## Requirements and run

- Go 1.22+ ([official installation](https://go.dev/doc/install))
- Python 3.9+
- No third-party Go/Python packages or API keys required.

The launcher also detects an optional checkout-local toolchain at `.tools/go/bin/go`. Toolchains, built binaries, databases, and caches are ignored by Git. A fresh clone requires Go on PATH or `GO_BIN`.

From this checkout:

```sh
python3 run.py
```

Open http://127.0.0.1:8765. Ctrl+C stops **both** services. The launcher builds `.bin/forecast-backend`, starts the Python service on loopback port 8766, waits for readiness, then starts the Go gateway on loopback port 8765. If either service fails, it stops the other. Existing `forecast.sqlite3` data is reused; no migration is needed. `python3 app.py` is now an internal-service entrypoint; use `run.py` for the dashboard.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `8765` | Go dashboard/API port |
| `PYTHON_PORT` | `8766` | Internal Python port; must differ from PORT |
| `FORECAST_DB` | `forecast.sqlite3` beside app.py | SQLite location; use absolute paths for a custom database |
| `GO_BIN` | `go` on PATH | Optional absolute Go executable path |
| `INGEST_WORKERS` | `2` | Concurrent upload-processing jobs (1–256) |
| `INGEST_CAPACITY` | `8` | Total admitted uploads, including running ones (1–256) |

For manually supervised services, both must receive the same randomly generated `FORECAST_INTERNAL_TOKEN`; Go also accepts a loopback `PYTHON_SERVICE_URL`. The launcher generates a new in-memory token each run. It is never stored in source, sent to the browser, or printed. This token isolates the internal API; the dashboard remains a single-user local prototype without login.

## Architecture

```text
Browser → Go gateway :8765
           ├─ project / forecast / search API → Python service :8766 → SQLite
           └─ upload → bounded channel → fixed goroutine workers → Python ingestion
                         ↑ job status / queued cancellation
```

- **Go** handles upload validation, request-size limits, queue backpressure, job status, queued cancellation, request contexts, HTTP timeouts, safe read retries, and shutdown.
- **Python** owns all structured data, atomic upload writes, chunking, lexical retrieval, scope approvals, and the forecasting baseline. The old per-upload Python thread pool is removed. HTTP handling remains threaded to allow independent requests while Go bounds ingestion work.
- Each worker handles **one upload batch**, containing up to 20 documents. This iteration does not schedule individual chunks or embedding calls. Multiple batches can overlap. Pure Python chunking is not promised a CPU speedup.
- Workers never share SQLite connections. Database connections close after each operation. Job payloads are cleared after dispatch/cancellation; retention is bounded to about 1,000 status records.
- API forwarding has a separate 16-request concurrency gate. Request bodies are capped at 2 MB. Both services bind only to loopback. The Go gateway validates Host and Origin.
- Reads may retry up to three attempts with short cancellable backoff. Writes are never automatically retried. A lost ingestion response becomes `unknown`, because the transaction might have committed. Check project evidence before submitting again.
- Cancelling a **queued** upload prevents processing. Running uploads return HTTP 409 on cancellation: the local Python transaction may already be committing. Client disconnection after acceptance does not cancel a job; it can be polled later.
- Job status is **in memory**. Restart loses pending jobs and status history, but completed document data persists in SQLite. Pending jobs are not durable/replayed. Cancelled queued jobs retain an admission slot until a worker drains them. This keeps queue memory bounded.

## Try the workflow

1. Load the fictional sample project and note the forecast.
2. Approve the customer-facing portal pivot and compare budget exposure.
3. Upload `.txt`, `.md`, or `.json` documents. The UI polls the Go job status; queued uploads can be cancelled.
4. Search “customer portal” and inspect source excerpts; attach evidence to a manually proposed scope change.
5. Import `task,hours` CSV time entries and update completed baseline hours.

### Job API

`POST /api/ingest` accepts the original JSON upload shape and now returns **202 Accepted**:

```json
{"project_id":1,"documents":[{"name":"brief.md","text":"Project requirements..."}]}
```

Response includes `id`, `project_id`, `status`, and `created_at`; the `Location` header points to `/api/jobs/{id}`. Poll with GET; successful jobs include `result.chunks`. DELETE cancels only queued jobs. Statuses: `queued`, `running`, `succeeded`, `failed`, `cancelled`, `unknown`. Full admission returns 429 with Retry-After; invalid input returns 400/413. Unknown projects return 404. `GET /api/health` checks both services. Other MVP endpoint shapes are unchanged.

## Forecast contract

Completed baseline hours are **original estimates for accepted work**, not hours logged. Remaining baseline = max(0, original planned hours − completed baseline hours). Observed effort ratio = actual / completed baseline hours, bounded to 0.5–3; defaults to 1 without useful progress observations. Remaining effort = max(0, (remaining baseline + approved remaining scope deltas) × observed ratio). Final hours = actual + remaining; final labor cost = final hours × blended cost rate.

Negative scope deltas remove remaining work, never hours already spent. The displayed 0.8× / 1.3× remaining-effort range is scenario sensitivity, **not** a calibrated confidence interval. No expense, staffing, calendar, or margin modeling yet.

## Validation

```sh
python3 -m unittest -v
go test -race ./...
python3 tests/integration.py
```

The integration test compiles the Go backend, starts both services against a temporary database on separate free loopback ports, exercises project creation, queued ingestion, source search, time imports, scope approval, persistence, and shutdown, and cleans up its processes. It does not touch your project database.

## Current limitations / next steps

Retrieval still uses keyword overlap; chunks are 220 words with 40-word overlap. No embeddings, vector database, LLM generation, trained forecasting model, or live GitHub/time-tracking connector is installed. JSON exports are indexed as plain text; PDF and DOCX parsing are not implemented. Imports append and can duplicate records. Source-document counts group same-name files.

Future Python ML modules can be called behind the same Go worker/API boundary. Add tokenizer-aware section chunking, batched embeddings, Chroma, cited answers, and evaluated extraction separately. Before remote connectors, add per-provider rate limits and idempotent job retries. Before multi-user hosting, add authentication, authorization, durable jobs, and persistent job recovery.
