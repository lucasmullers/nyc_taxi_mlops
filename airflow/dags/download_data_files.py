import requests
from pendulum import datetime
from airflow.decorators import task, dag
from airflow.models.dagrun import DagRun


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


@dag(
    dag_id="download_nyc_taxi_data",
    schedule="@monthly",
    start_date=datetime(2023, 1, 1, tz="UTC"),
    catchup=True,
    tags=["extract", "nyc_taxi"],
    max_active_runs=1,
)
def my_taskflow_dag():
    @task()
    def download_nyc_taxi_data(dag_run: DagRun) -> str:
        print("Downloading data for: ", dag_run.logical_date)

        date = dag_run.logical_date.strftime("%Y-%m")
        filename = f"yellow_tripdata_{date}.parquet"
        response = requests.get(BASE_URL + filename)
        with open(f"/opt/airflow/data/{filename}", "wb") as f:
            f.write(response.content)

        print("File downloaded and saved with name: ", filename)

        return filename

    download_nyc_taxi_data()


my_taskflow_dag()
