import requests
import boto3
from tempfile import NamedTemporaryFile

from pendulum import datetime
from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


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
        def upload_to_s3(local_file_path: str, prefix: str, filename: str):
            s3_hook = S3Hook(aws_conn_id="aws_default")
            s3_client = s3_hook.get_conn()

            if not prefix.endswith("/"):
                prefix += "/"

            print("Uploading file: ", local_file_path, " to prefix: ", prefix + filename)
            s3_client.upload_file(local_file_path, "nyc-mlops-data", prefix + filename)

        print("Downloading data for: ", execution_date)
        date = execution_date[:7]
        filename = f"yellow_tripdata_{date}.parquet"
        response = requests.get(BASE_URL + filename)
        with NamedTemporaryFile() as tmp_file:
            tmp_file.write(response.content)
            upload_to_s3(tmp_file.name, "landing_zone/", filename)
            tmp_file.close()

        print("File downloaded and saved with name: ", filename)
        return filename

    end = EmptyOperator(task_id="end")

    start >> download_nyc_taxi_data(execution_date="{{ prev_execution_date }}") >> end


my_taskflow_dag()
