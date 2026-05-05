# DAX/MDX to Databricks Metric View Translator

A Databricks App that translates Power BI DAX measures and SSAS MDX calculated members into [Databricks Metric Views](https://docs.databricks.com/en/sql/language-manual/sql-ref-metric-view.html) using Claude via the [Databricks Foundation Model API (FMAPI)](https://docs.databricks.com/en/machine-learning/model-serving/score-foundation-models.html).

No API keys required — the app uses Databricks FMAPI for LLM inference and On-Behalf-Of (OBO) authentication for all SQL operations.

## What It Does

Paste your DAX or MDX measure definitions, point at a source table, and the app:

1. **Translates** measures into Metric View YAML using Claude (via FMAPI)
2. **Generates** a ready-to-run `CREATE OR REPLACE VIEW ... WITH METRICS` SQL statement
3. **Deploys** the metric view directly to your workspace (one click)
4. **Queries** the deployed metric view to validate results
5. **Logs** all translations and deployments to an audit Delta table

### Supported DAX/MDX Patterns

| Pattern | Examples |
|---------|----------|
| Simple aggregations | `SUM`, `COUNT`, `DISTINCTCOUNT`, `AVERAGE`, `MIN`, `MAX` |
| Filtered measures | `CALCULATE(SUM(...), Table[Col]="X")` / MDX tuple filters |
| Row-level calculations | `SUMX(T, T[A]*T[B])` |
| Safe division | `DIVIDE(num, den, 0)` |
| Star-schema joins | `RELATED(DimTable[Col])` / MDX dimension references |
| Composite measures | `[Measure1] / [Measure2]` (auto-inlined) |
| Window measures | `TOTALYTD(...)`, `PeriodsToDate(...)` (version 0.1) |
| Unsupported patterns | `RANKX`, `USERELATIONSHIP`, etc. produce warnings with best-effort approximations |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Databricks App                                     │
│                                                     │
│  ┌──────────────┐     ┌──────────────────────────┐  │
│  │ React        │────>│ FastAPI Backend           │  │
│  │ Frontend     │<────│                          │  │
│  │ (Vite + TW)  │     │  /api/translate/text     │  │
│  └──────────────┘     │  /api/deploy             │  │
│                       │  /api/query              │  │
│                       │  /api/audit              │  │
│                       └──────────┬───────────────┘  │
│                                  │                  │
└──────────────────────────────────┼──────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
              ┌─────▼─────┐ ┌─────▼─────┐ ┌─────▼─────┐
              │ FMAPI     │ │ SQL       │ │ Audit     │
              │ Claude    │ │ Warehouse │ │ Delta     │
              │ Serving   │ │ (OBO)     │ │ Table     │
              │ Endpoint  │ │           │ │           │
              └───────────┘ └───────────┘ └───────────┘
```

- **Frontend**: React 18 + Tailwind CSS (dark/light mode, YAML/SQL editor, deploy + query panels)
- **Backend**: FastAPI — translations use the app service principal; deploy/query run as the logged-in user via OBO
- **Translation**: Claude via Databricks FMAPI — no Anthropic API key needed
- **Deployment**: Statement Execution REST API with user's forwarded OAuth token

## Prerequisites

1. **Databricks workspace** with Unity Catalog enabled
2. **Databricks CLI** v0.230.0+ — [install guide](https://docs.databricks.com/en/dev-tools/cli/install.html)
3. **SQL Warehouse** (serverless recommended) — you'll need its ID
4. **Claude serving endpoint** — a Foundation Model API endpoint such as `databricks-claude-sonnet-4-6`. Verify it exists:
   ```bash
   databricks serving-endpoints list --output json | grep -i claude
   ```
   If no Claude endpoint exists, [create one](https://docs.databricks.com/en/machine-learning/model-serving/create-foundation-model-endpoints.html) or use any available Claude FMAPI endpoint.

## Deploy as a Databricks App

### Step 1: Authenticate to your workspace

```bash
databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com
```

Verify you're connected:

```bash
databricks current-user me
```

### Step 2: Clone the repo

```bash
git clone https://github.com/danny-db/metric-view-translator.git
cd metric-view-translator
```

### Step 3: Find your SQL Warehouse ID

```bash
databricks warehouses list
```

Copy the ID of the warehouse you want to use (serverless recommended).

### Step 4: Deploy

```bash
databricks bundle deploy -t dev \
  --var="warehouse_id=YOUR_WAREHOUSE_ID"
```

Optional: override the serving endpoint or catalog:

```bash
databricks bundle deploy -t dev \
  --var="warehouse_id=abc123def456" \
  --var="serving_endpoint=databricks-claude-sonnet-4-6" \
  --var="target_catalog=my_catalog"
```

### Step 5: Open the app

After deployment, the app starts automatically. Get the URL:

```bash
databricks apps get metric-view-translator
```

Open the `url` field from the output in your browser. First launch may take 1-2 minutes to start.

If the app isn't running, start it manually:

```bash
databricks apps start metric-view-translator
```

### Step 6: Use the app

1. Select **DAX** or **MDX** mode
2. Enter your source table (e.g., `main.default.fact_sales`)
3. Paste your measure definitions
4. Optionally add dimension tables for star-schema joins
5. Click **Translate** — review the generated YAML and SQL
6. Click **Deploy** to create the metric view in your catalog
7. Click **Query** to validate the results

## Notebooks (No App Needed)

The repo includes standalone Databricks notebooks that run the translation engine directly:

| Notebook | Description |
|----------|-------------|
| `notebooks/dax_translator_notebook.py` | Interactive DAX translation with 6 difficulty levels and automated validation |
| `notebooks/mdx_translator_notebook.py` | Interactive MDX translation with 6 difficulty levels and automated validation |
| `notebooks/batch/batch_translator.py` | Batch translate from Power BI `.bim` exports, DAX Studio CSV, or MDX scripts |

### Using the notebooks

1. Import the notebook into your Databricks workspace
2. Attach to a cluster with **DBR 15.4+** (Python 3.11+)
3. Update the `TARGET_CATALOG` variable in the configuration cell to your catalog
4. Run all cells — the notebook creates test data, translates, deploys, and validates

Each notebook is fully self-contained: no external packages beyond `pydantic` and `pyyaml` (installed via `%pip install` in the first cell).

## Configuration

### Bundle variables (`databricks.yml`)

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `warehouse_id` | SQL warehouse ID | _(none)_ | **Yes** |
| `serving_endpoint` | Claude FMAPI endpoint name | `databricks-claude-sonnet-4-6` | No |
| `target_catalog` | Catalog for metric views | `main` | No |
| `target_schema` | Schema for metric views | `dax_translator_test` | No |

### Runtime config (in-app)

The app's **Config** tab lets you override at runtime without redeploying:
- Audit table location (catalog/schema/table)
- Serving endpoint name
- Custom prompt suffixes for DAX/MDX translation

## Project Structure

```
dax-to-metric-view/
├── app/                          # Databricks App (deployed to workspace)
│   ├── app.yaml                  # App runtime config
│   ├── requirements.txt          # Python dependencies for the app
│   ├── backend/                  # FastAPI backend
│   │   ├── main.py               # App entry point
│   │   ├── routes.py             # API endpoints
│   │   ├── translator_service.py # DAX/MDX → Metric View (FMAPI)
│   │   ├── deployer_service.py   # OBO SQL execution for deploy/query
│   │   ├── audit_service.py      # Translation audit trail (Delta)
│   │   ├── config.py             # Runtime configuration
│   │   └── models.py             # Pydantic request/response models
│   └── frontend/
│       ├── src/                  # React + TypeScript source
│       └── dist/                 # Pre-built static files (committed)
├── notebooks/                    # Standalone Databricks notebooks
│   ├── dax_translator_notebook.py
│   ├── mdx_translator_notebook.py
│   └── batch/
│       ├── batch_translator.py   # Batch translation with ai_query()
│       └── samples/              # Sample BIM, CSV, MDX input files
├── src/                          # CLI / library (local development only)
├── tests/                        # Unit and integration tests
├── databricks.yml                # Databricks Asset Bundle definition
├── pyproject.toml                # Python project metadata
└── .env.example                  # Environment template (local dev)
```

## DAX-to-Metric View Quick Reference

| DAX | Metric View SQL |
|-----|----------------|
| `SUM(Sales[Amount])` | `SUM(amount)` |
| `DISTINCTCOUNT(Sales[CustomerID])` | `COUNT(DISTINCT customer_id)` |
| `CALCULATE(SUM(...), Sales[Status]="Active")` | `SUM(amount) FILTER (WHERE status = 'Active')` |
| `SUMX(Sales, Sales[Qty]*Sales[Price])` | `SUM(quantity * unit_price)` |
| `DIVIDE(SUM(Profit), SUM(Revenue), 0)` | `SUM(profit) / NULLIF(SUM(revenue), 0)` |
| `RELATED(DimCustomer[Name])` | `customer.name` (with join defined) |
| `TOTALYTD(SUM(Sales[Amount]), Calendar[Date])` | Window measure: cumulative + current year |
| `RANKX(...)` | Warning: unsupported |

## Local Development

```bash
# Install Python dependencies
pip install uv
uv sync

# Run unit tests
uv run pytest tests/unit/ -v

# Build frontend (only if modifying the React app)
cd app/frontend && npm install && npm run build && cd ../..

# Run backend locally (requires DATABRICKS_HOST + auth configured)
uv run uvicorn app.backend.main:app --reload --port 8000
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `bundle deploy` fails with "warehouse_id is required" | Pass `--var="warehouse_id=YOUR_ID"` |
| App shows "Missing user token" | Open the app URL in a browser where you're logged into Databricks |
| Translation returns empty results | Verify the Claude serving endpoint exists: `databricks serving-endpoints get databricks-claude-sonnet-4-6` |
| Deploy fails with storage permission error | The audit table creation requires write access. Run the CREATE TABLE DDL from a notebook first (printed in the error message) |
| App not starting | Check logs: `databricks apps get-logs metric-view-translator` |

## License

Apache 2.0 — see [LICENSE](LICENSE)
