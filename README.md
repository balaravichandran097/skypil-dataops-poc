# SkyPIL DataOps — Live Demo

FastAPI + SQLite single-worker FIFO execution demo. Includes task discovery, dynamic task metadata, CSV uploads, dry-run, production-style approval simulation, job status, audit trail and CSV extracts.

## Run

```bash
pip install -r requirements.txt
export SKYPIL_DEMO_PASSWORD=choose-a-secret
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 1
```

Log in using HTTP Basic username `demo` and your configured password.

CSV example: `account_id\nA001\nA002\n` (partner `demo`).

**Security:** demonstration only; no SSO, durable backing store, or AWS execution. Do not connect it to production databases. Render free instances may lose SQLite data when restarted.
