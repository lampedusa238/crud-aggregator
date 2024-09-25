from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from datetime import timedelta

default_args = {
    'depends_on_past': False,
    'start_date':days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'aggregate_logs',
    default_args=default_args,
    description='Простой DAG для запланированного запуска Spark-скрипта агрегации CRUD-логов по пользователям',
    schedule_interval='0 7 * * *',  # каждый день в 7 утра по UTC
    # schedule_interval='0 4 * * *',  # каждый день в 7 утра по MSK
    catchup=False,
)

t1 = BashOperator(
    task_id='aggregate_logs_task',
    bash_command="""
    docker exec crud-aggregator-spark-master-1 \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /scripts/spark_script.py
    """,
    dag=dag,
)

t1
