from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="twitter_sentiment_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start_stream = BashOperator(
        task_id="start_streaming_job",
        bash_command="python spark_streaming/streaming_job.py"
    )
