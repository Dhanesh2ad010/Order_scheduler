
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, Variable, task
from airflow.utils.trigger_rule import TriggerRule

CONFIG_FILE = Path(__file__).parent / "config" / "pipelines.json"
REQUIRED_COLUMNS = ["order_id", "customer_id", "product", "amount", "order_date"]


def row_problems(row):
    """Return a list of validation problems for one CSV row."""
    if None in row or None in row.values():
        return ["wrong number of columns"]

    problems = []
    for col in REQUIRED_COLUMNS:
        if not row[col].strip():
            problems.append(f"{col} is empty")

    if row["order_id"].strip() and not row["order_id"].strip().isdigit():
        problems.append("order_id is not an integer")

    if row["amount"].strip():
        try:
            if float(row["amount"]) <= 0:
                problems.append("amount must be > 0")
        except ValueError:
            problems.append(f"amount '{row['amount']}' is not numeric")

    if row["order_date"].strip():
        try:
            datetime.strptime(row["order_date"].strip(), "%Y-%m-%d")
        except ValueError:
            problems.append(f"order_date '{row['order_date']}' is not a valid date")

    return problems


def create_dag(cfg):
    conn_id = cfg["postgres_conn_id"]

    def file_name(ds):
        return f"{cfg['file_prefix']}{ds}{cfg['file_ext']}"

    # ---- callback --------------------------------------------------------
    def mark_file_failed(context):
        """on_failure_callback: record FAILED (never overwrites a SUCCESS)."""
        PostgresHook(conn_id).run(
            """
            INSERT INTO processed_files (file_name, status) VALUES (%s, 'FAILED')
            ON CONFLICT (file_name) DO UPDATE
                SET status = 'FAILED', processed_at = NOW()
                WHERE processed_files.status <> 'SUCCESS'
            """,
            parameters=(file_name(context["ds"]),),
        )

    # Airflow 3 removed SLAs, so a task timeout is used to catch slow runs.
    default_args = {
        "retries": cfg["retries"],
        "retry_delay": timedelta(minutes=cfg["retry_delay_minutes"]),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=cfg["task_timeout_minutes"]),
    }

    with DAG(
        dag_id=cfg["dag_id"],
        schedule=cfg["schedule"],
        start_date=datetime.fromisoformat(cfg["start_date"]),
        catchup=cfg["catchup"],       # backfill: one run (and one file) per date
        max_active_runs=1,
        default_args=default_args,
        tags=["orders", "etl"],
    ) as dag:

        # Sensor: waits for the day's file. Missing file -> task fails (visible in UI).
        wait_for_file = FileSensor(
            task_id="wait_for_file",
            fs_conn_id="fs_default",
            filepath=(
                "{{ var.value." + cfg["input_dir_variable"] + " }}/"
                + cfg["file_prefix"] + "{{ ds }}" + cfg["file_ext"]
            ),
            poke_interval=10,
            timeout=cfg["sensor_timeout_seconds"],
            mode="reschedule",
        )

        @task(on_failure_callback=mark_file_failed)
        def validate(ds=None):
            path = Path(Variable.get(cfg["input_dir_variable"])) / file_name(ds)
            errors, rows, seen_ids = [], [], set()

            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    raise AirflowFailException(f"{path.name} is empty")
                missing = set(REQUIRED_COLUMNS) - set(reader.fieldnames)
                if missing:
                    raise AirflowFailException(f"Missing columns: {sorted(missing)}")

                for line, row in enumerate(reader, start=2):
                    problems = row_problems(row)
                    order_id = (row.get("order_id") or "").strip()
                    if order_id in seen_ids:
                        problems.append("duplicate order_id in file")
                    seen_ids.add(order_id)

                    if problems:
                        errors.append(f"line {line}: {'; '.join(problems)}")
                    else:
                        rows.append(tuple(row[c].strip() for c in REQUIRED_COLUMNS))

            if not rows and not errors:
                raise AirflowFailException(f"{path.name} has no data rows")
            if errors:
                # FailException = no point retrying bad data
                raise AirflowFailException(f"{path.name} invalid:\n" + "\n".join(errors))

            return {"file_name": path.name, "rows": rows}   # passed on via XCom

        @task.branch
        def check_duplicate_file(info):
            already_done = PostgresHook(conn_id).get_first(
                "SELECT 1 FROM processed_files WHERE file_name = %s AND status = 'SUCCESS'",
                parameters=(info["file_name"],),
            )
            return "skip_load" if already_done else "load"

        @task(on_failure_callback=mark_file_failed)
        def load(info):
            """One transaction: orders + file status commit together or not at all."""
            conn = PostgresHook(conn_id).get_conn()
            try:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO orders (order_id, customer_id, product, amount, order_date)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO NOTHING
                        """,
                        [tuple(r) for r in info["rows"]],
                    )
                    cur.execute(
                        """
                        INSERT INTO processed_files (file_name, status) VALUES (%s, 'SUCCESS')
                        ON CONFLICT (file_name) DO UPDATE
                            SET status = 'SUCCESS', processed_at = NOW()
                        """,
                        (info["file_name"],),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

        skip_load = EmptyOperator(task_id="skip_load")
        done = EmptyOperator(
            task_id="done",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # runs after load OR skip
        )

        info = validate()
        branch = check_duplicate_file(info)
        load_task = load(info)

        wait_for_file >> info >> branch >> [load_task, skip_load] >> done

    return dag


# Dynamic DAG generation: one DAG per entry in the config file.
for pipeline_cfg in json.loads(CONFIG_FILE.read_text()):
    globals()[pipeline_cfg["dag_id"]] = create_dag(pipeline_cfg)