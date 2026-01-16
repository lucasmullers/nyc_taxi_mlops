from pathlib import Path
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from cosmos import ProjectConfig, ProfileConfig, DbtTaskGroup

# https://support.astronomer.io/hc/en-us/articles/23757501934099-DagBag-Import-timeout-on-worker-using-cosmos
DBT_ROOT_PATH = Path(Path(__file__).parent.parent / "dbt")

@dag(
    schedule_interval="@monthly",
    start_date=datetime(2025, 2, 1, tz="UTC"),
    catchup=True,
    dag_id="taxi_ml_pipeline",
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["prepare_data_to_ml", "nyc_taxi"]
)
def taxi_ml_pipeline():
    @task
    def export_duckdb_to_parquet(data_interval_start, data_interval_end):
        import duckdb

        conn = duckdb.connect(f"{DBT_ROOT_PATH}/local.duckdb")
        conn.sql(
            f"COPY (SELECT * FROM main.taxi_ml_features WHERE pickup_datetime >= '{data_interval_start}' AND pickup_datetime < '{data_interval_end}') TO '{DBT_ROOT_PATH}/../data/processed/taxi_ml_features_{data_interval_start}.parquet' WITH (FORMAT 'parquet', COMPRESSION 'snappy')"
        )

    start = EmptyOperator(task_id="start")
    profile_config = ProfileConfig(
        profile_name="nyc_taxi",
        target_name="dev",
        profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml",
    )
    project_config = ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH,
        models_relative_path="models",
        manifest_path=DBT_ROOT_PATH / "target/manifest.json",
        dbt_vars={
            "data_interval_start": "{{ prev_ds }}",
            "data_interval_end": "{{ ds }}",
        },
    )

    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
    )
    end = EmptyOperator(task_id="end")

    # start >> dbt_running_models >> end
    start >> dbt_running_models >> export_duckdb_to_parquet(data_interval_start="{{ prev_ds }}", data_interval_end="{{ ds }}") >> end

taxi_ml_pipeline()