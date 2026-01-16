# Azure CSP Billing Portal

A FastAPI-based web portal for Cloud Solution Providers to ingest large Azure usage CSV exports, apply customer-specific pricing strategies, and share interactive consumption dashboards with their users. The application stores metadata in SQLite, performs analytical queries with DuckDB and Polars, and delivers a modern Azure-themed UI with Tabulator and Chart.js.

## Features

- **Role-based access** with admin-managed uploads and analyst read access.
- **Large CSV ingestion** (100-300 MB) streamed to disk, compressed to Parquet, and queried via DuckDB.
- **Dynamic financial columns** including forex adjustments, margin controls, and VAT calculations.
- **Interactive table** with search, column visibility, filtering, pagination, and CSV export.
- **Dashboards** surfacing top customers, daily spend trends, and configurable billing summaries.
- **Container-ready** deployment targeting Azure App Service for Containers.

## Getting Started

### Prerequisites

- Python 3.11+
- Docker (for container builds)
- PowerShell or Bash shell

### Local Development

```powershell
# create virtual environment
python -m venv .venv

# activate
.\.venv\Scripts\Activate.ps1

# install dependencies
pip install -r requirements.txt

# set environment variables (optional)
$env:UVICORN_RELOAD = "1"

# run the app
uvicorn app.main:app --reload
```

Open `http://localhost:8000` and log in with a seeded account (see below).

### Creating Seed Users

Use the helper script below to create an initial admin and analyst:

```powershell
python -m app.seed_users --email admin@contoso.com --password "YourStrongPassword" --role admin --name "Admin User"
python -m app.seed_users --email analyst@contoso.com --password "AnotherStrongPassword" --role analyst --name "Analyst User"
```

### Docker

```powershell
# build
docker build -t csp-billing:latest .

# run
docker run -p 8000:8000 -v ${PWD}/data:/app/data csp-billing:latest
```

Mounting the `data/` directory preserves uploads across restarts.

**Tagging convention:** when creating Docker image tags or release tags, follow the pattern `vX.Y` where `Y` ranges from `0` to `9` only (e.g., `v1.9`, `v2.0`, `v2.9`, `v3.0`). Do not publish tags using double-digit patch numbers such as `v2.10` or `v2.11`.

## Azure Deployment Guidance

1. **Container Registry**: Push the built image to Azure Container Registry (`az acr build` or `docker push`).
2. **App Service Plan**: Create a Linux App Service plan sized for memory-intensive workloads (at least P1v3 recommended for 300 MB CSV processing).
3. **Web App**: Deploy the container image to Azure App Service. Configure settings:
   - `WEBSITES_PORT=8000`
   - `SCM_DO_BUILD_DURING_DEPLOYMENT=false`
   - Optional: `APPINSIGHTS_INSTRUMENTATIONKEY`
4. **Storage**: Consider mounting Azure Files to `/app/data` for durable upload storage and parquet warehouse files.
5. **Monitoring**: Enable App Insights and configure alerts for error rates and container restarts.

## Configuration

Environment variables (with defaults from `app/config.py`):

| Variable | Description |
|----------|-------------|
| `SECRET_KEY` | Session encryption secret. Change in production. |
| `DATABASE_URL` | SQLModel connection string (default SQLite). |
| `UPLOADS_DIR` | Path to store raw CSV uploads. |
| `PROCESSED_DIR` | Path to parquet warehouse outputs. |
| `DEFAULT_VAT` | Default VAT multiplier (1.12). |

Override via `.env` file or App Service configuration.

## Testing

Add tests under `tests/` (not yet provided). Recommended stacks: `pytest` for API routes and integration tests covering ingestion logic with sample CSV fixtures.

---

Made with 💙 for Azure CSPs.
