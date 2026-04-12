# agentopia-knowledge-ingest
Upstream knowledge ingestion pipeline for Agentopia. Document Ingest Service, Normalizer, Metadata Extractor, Ingest Orchestrator, and Operator UI. Feeds into agentopia-super-rag (retrieval plane).

## Running Tests

### Prerequisites

- Python 3.11+ with dependencies installed:
  ```
  pip install -r requirements.txt
  ```
- Node.js >= 16 (ubuntu-latest runners include Node.js; required by `test_gdrive_picker_ui.py` and `test_generic_connector_ui.py` which invoke `node -e` for UI JS execution tests)

No real database, S3, or Google API credentials are needed — all external calls are mocked or stubbed.

### Canonical smoke command

```
PYTHONPATH=src pytest tests/ -q --tb=short
```

`PYTHONPATH=src` is required because `test_aws_s3_wrapper.py` imports modules directly from `src/`. Without it, pytest collection raises an `ImportError` for that file.

This command is the same one used in the [PR validation workflow](.github/workflows/pr-check.yml).
