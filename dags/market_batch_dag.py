from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="market_batch_analytics_dag",
    default_args=default_args,
    description="Trigger Spark Batch Analytics process on streamed MinIO data",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["spark", "batch", "minio"],
) as dag:

    run_spark_batch_task = BashOperator(
        task_id="run_spark_batch_analytics",
        bash_command="python /opt/airflow/dags/run_batch_analytics.py",
    )

    run_spark_batch_task