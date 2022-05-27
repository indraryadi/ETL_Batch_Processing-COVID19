from asyncio import tasks
from email.policy import default
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime


default_arg={
    'owner' : 'indra',
    'depend_on_past':False,
    'start_date':datetime(2022,5,25)
}

with DAG(
    dag_id='covid19',
    schedule_interval='@daily',
    default_args=default_arg
 ) as dag:
    
    start= DummyOperator(
        task_id="start"
    )
    
    raw_to_hdfs=BashOperator(
        task_id="raw_to_hdfs",
        bash_command="~/spark-3.0.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev ~/Documents/ETL_Batch_Processing-COVID19/ingest.py"
    )
    
    stop= DummyOperator(
        task_id="stop"
    )
start>>raw_to_hdfs>>stop
