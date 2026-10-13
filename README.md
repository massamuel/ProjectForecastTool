# ProjectForecastTool

Local project effort and labor-cost forecasting MVP. Python 3.9+; no third-party dependencies.

## Run

```sh
cd /Users/sampoplack/Documents/CODING_PROJECTS/ProjectForecastTool
python3 app.py
```

Open http://127.0.0.1:8765. Stop with Ctrl+C. Set `PORT` to change the port, or `FORECAST_DB` to use another SQLite database path. The server binds only to loopback. This is a single-user local prototype, without authentication.

## Try the workflow

1. Load the fictional sample project.
2. Note the initial forecast, then approve the customer-portal pivot.
3. Search “customer portal” and inspect its source evidence.
4. Upload `.txt`, `.md`, or `.json` project documents, then attach retrieved evidence to a scope proposal.
5. Import time entries with `task,hours` CSV columns and update completed baseline hours.
6. Refresh the browser to verify persistence.

## Forecast contract

Completed baseline hours are the **original estimates for accepted work**, not actual logged hours. Remaining baseline work = max(0, original planned hours − completed baseline hours). Observed effort ratio = actual hours / completed baseline hours, bounded to 0.5–3; defaults to 1 when no useful progress observation exists. Remaining effort = max(0, (remaining baseline work + approved remaining scope deltas) × observed effort ratio). Final hours = actual + remaining; final labor cost = final hours × blended labor cost rate.

Negative scope changes remove remaining work, never money already spent. ± scenario bounds use 0.8× / 1.3× remaining effort and are **not calibrated confidence intervals**. Rates represent labor cost, not billing revenue. No expense, staffing, calendar, or margin modeling yet.

## Architecture and concurrency

Python ThreadingHTTPServer → SQLite persistence → document ingestion using a bounded four-worker ThreadPoolExecutor. At most two ingestion operations run at once, with at most 20 documents per operation and a 2 MB HTTP request cap. Documents use 220-word chunks with 40-word overlap. Workers return results; database writes occur in one request-owned transaction. The threading implementation is an educational concurrency scaffold; pure Python chunking does not promise a CPU speedup. Network API operations are the intended next I/O workload.

Retrieval ranks chunks by unique query-word overlap and returns source IDs and excerpts. It is lexical retrieval, without embeddings or LLM-generated answers. Scope proposals and effort estimates are entered by a human and require approval. No trained ML, automatic scope extraction, external API integration, PDF/DOCX parsing, or durable background jobs yet. Raw JSON is indexed as text. Uploads and time imports append; repeated uploads may duplicate records. Use descriptive unique document names; same-name sources are counted together.

## Verification

```sh
python3 -m unittest -v
```

Tests use a temporary SQLite database and cover forecasts, approvals, scope removal, ingestion/retrieval isolation, and transactional CSV validation.

Next milestones: idempotent imports, dated forecast snapshots, task-level progress, async connectors with token-aware rate limits, embedding retrieval, evaluated requirement extraction, and trained forecasting against historical outcomes with project/time-separated evaluation.
