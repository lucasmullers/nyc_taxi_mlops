import requests
from pendulum import datetime
from airflow.decorators import task, dag
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


@dag(
    dag_id="download_nyc_taxi_data",
    schedule="@monthly",
    start_date=datetime(2025, 2, 1, tz="UTC"),
    catchup=True,
    tags=["extract", "nyc_taxi"],
    max_active_runs=1,
)
def my_taskflow_dag():
    start = EmptyOperator(task_id="start")

    @task()
    def download_nyc_taxi_data(execution_date: str) -> str:
        print("Downloading data for: ", execution_date)

        date = execution_date[:7]
        filename = f"yellow_tripdata_{date}.parquet"
        response = requests.get(BASE_URL + filename)
        with open(f"/opt/airflow/data/landing_zone/{filename}", "wb") as f:
            f.write(response.content)

        print("File downloaded and saved with name: ", filename)

        return filename

    end = EmptyOperator(task_id="end")

    start >> download_nyc_taxi_data(execution_date="{{ prev_execution_date }}") >> end


my_taskflow_dag()
