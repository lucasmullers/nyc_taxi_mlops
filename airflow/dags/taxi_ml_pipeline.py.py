import os
from datetime import datetime
from pathlib import Path
from cosmos import ProjectConfig, ProfileConfig
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task


DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))


@dag(
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="taxi_ml_pipeline",
    default_args={"retries": 0},
)
def taxi_ml_pipeline():
    @task
    def export_duckdb_to_parquet():
        import duckdb

        conn = duckdb.connect(f"{DBT_ROOT_PATH}/local.duckdb")
        conn.sql(
            f"COPY main.taxi_ml_features TO '{DBT_ROOT_PATH}/../data/processed/taxi_ml_features.parquet' WITH (FORMAT 'parquet', COMPRESSION 'snappy')"
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
        # env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=DBT_ROOT_PATH,
    )

    dbt_run >> export_duckdb_to_parquet()


taxi_ml_pipeline()
