import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from transformers.getSpotifyData import SpotifyToGCS
from handles.googleCloudStorageHandle import GCSHandle
from handles.spotifyHandle import SpotifyHandle


default_args = {"owner": "airflow"}

if "LAST_YEAR" in os.environ:
    cur_year = str(int(last_year) + 1)
else:
    cur_year = "2010"
    os.environ["LAST_YEAR"] = cur_year

with DAG(
    dag_id="extract_load_year_spotify",
    start_date=datetime(2021, 5, 20),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    gcs = GCSHandle()
    spopy = SpotifyHandle()
    # Spotify API transformer
    sp = SpotifyToGCS(spopy, gcs)

    # Configure the load_ArqOrdemServico_to_postgres task
    extract_load = PythonOperator(
        task_id="extract_load_year_spotify",
        python_callable=sp.run,
        op_kwargs={
            "type": "year",
            "value": cur_year,
            "limit": 50,
            "bucket": "dsp_project",
        },
    )

    extract_load

if __name__ == "__main__":
    print("Test not implemented...")
