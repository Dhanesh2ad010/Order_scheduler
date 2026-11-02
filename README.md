# Order Scheduler — Airflow ETL Pipeline

An Apache Airflow DAG that automates daily ingestion of order CSV files into a PostgreSQL database. It validates incoming data, avoids duplicate loads, and gracefully tracks processing status — all driven by a JSON config file for easy multi-pipeline scaling.

---

## Project Structure

```
order_scheduler/
├── order_pipeline.py        # Core DAG definition (dynamic DAG factory)
└── config/
    └── pipelines.json       # Pipeline configuration (one entry per DAG)
```

---

## How It Works

Each entry in `config/pipelines.json` generates a separate Airflow DAG. The pipeline for each DAG follows these steps:

```
wait_for_file → validate → check_duplicate_file → load → done
                                                 ↘ skip_load ↗
```

| Task | Description |
|------|-------------|
| `wait_for_file` | `FileSensor` that polls for the day's CSV file (e.g. `orders_2026-09-25.csv`) |
| `validate` | Checks columns, empty values, types, date formats, and duplicate `order_id`s |
| `check_duplicate_file` | Branch: skips loading if the file was already successfully processed |
| `load` | Inserts orders into PostgreSQL in a single atomic transaction |
| `skip_load` | No-op branch taken when the file was already processed |
| `done` | Terminal task — runs after either `load` or `skip_load` |

---

## Database Tables Required

```sql
-- Orders table
CREATE TABLE orders (
    order_id    INTEGER PRIMARY KEY,
    customer_id TEXT    NOT NULL,
    product     TEXT    NOT NULL,
    amount      NUMERIC NOT NULL CHECK (amount > 0),
    order_date  DATE    NOT NULL
);

-- File tracking table
CREATE TABLE processed_files (
    file_name    TEXT PRIMARY KEY,
    status       TEXT NOT NULL,          -- 'SUCCESS' | 'FAILED'
    processed_at TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Configuration (`config/pipelines.json`)

```json
[
  {
    "dag_id":               "orders_pipeline",
    "schedule":             "@daily",
    "start_date":           "2026-09-25",
    "catchup":              true,
    "input_dir_variable":   "orders_input_dir",
    "file_prefix":          "orders_",
    "file_ext":             ".csv",
    "postgres_conn_id":     "db_conn",
    "retries":              2,
    "retry_delay_minutes":  1,
    "sensor_timeout_seconds": 60,
    "task_timeout_minutes": 30
  }
]
```

| Key | Description |
|-----|-------------|
| `dag_id` | Unique Airflow DAG identifier |
| `schedule` | Cron expression or preset (e.g. `@daily`) |
| `start_date` | Backfill start date (`YYYY-MM-DD`) |
| `catchup` | If `true`, Airflow runs one DAG per past date since `start_date` |
| `input_dir_variable` | Airflow Variable name that holds the input directory path |
| `file_prefix` / `file_ext` | File naming pattern: `{prefix}{ds}{ext}` (e.g. `orders_2026-09-25.csv`) |
| `postgres_conn_id` | Airflow Connection ID for PostgreSQL |
| `retries` | Number of task-level retries |
| `retry_delay_minutes` | Delay between retries (with exponential backoff) |
| `sensor_timeout_seconds` | Max seconds `FileSensor` waits before failing |
| `task_timeout_minutes` | Max execution time per task |

---

## Setup & Prerequisites

### 1. Airflow Connections

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `db_conn` | `postgres` | PostgreSQL database |
| `fs_default` | `fs` | Base path for file sensor |

### 2. Airflow Variables

| Variable | Example Value | Description |
|----------|---------------|-------------|
| `orders_input_dir` | `/data/orders` | Directory where daily CSV files are placed |

### 3. Install Provider Packages

```bash
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-standard
```

---

## Expected CSV Format

Files must be named `orders_YYYY-MM-DD.csv` and contain these columns:

| Column | Type | Rules |
|--------|------|-------|
| `order_id` | Integer | Required, unique within file |
| `customer_id` | String | Required, non-empty |
| `product` | String | Required, non-empty |
| `amount` | Float | Required, must be > 0 |
| `order_date` | Date `YYYY-MM-DD` | Required, valid date |

**Example:**
```csv
order_id,customer_id,product,amount,order_date
1001,CUST-01,Widget A,49.99,2026-09-25
1002,CUST-02,Widget B,120.00,2026-09-25
```

---

## Adding More Pipelines

To add a second pipeline (e.g. for returns), simply append a new entry to `pipelines.json`:

```json
[
  { ... existing orders pipeline ... },
  {
    "dag_id":               "returns_pipeline",
    "schedule":             "@daily",
    "start_date":           "2026-10-01",
    "catchup":              false,
    "input_dir_variable":   "returns_input_dir",
    "file_prefix":          "returns_",
    "file_ext":             ".csv",
    "postgres_conn_id":     "db_conn",
    "retries":              1,
    "retry_delay_minutes":  2,
    "sensor_timeout_seconds": 120,
    "task_timeout_minutes": 20
  }
]
```

No code changes needed — a new DAG will appear in the Airflow UI automatically.

---

## Error Handling

- **Validation failures** → `AirflowFailException` (no retries, bad data won't be retried)
- **Load failures** → Transaction is rolled back, failure recorded in `processed_files`
- **Duplicate file** → Detected via `processed_files` table; load is safely skipped
- **Slow tasks** → `execution_timeout` kills tasks that exceed `task_timeout_minutes`

---

## License

MIT