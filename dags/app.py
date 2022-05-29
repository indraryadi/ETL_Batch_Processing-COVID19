from asyncio import tasks
from email.policy import default
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, datetime


default_arg={
    'owner' : 'indra',
    'depend_on_past':False,
    'start_date':datetime(2022,5,29)
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
        bash_command="~/spark-3.0.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev ~/Documents/ETL_Batch_Processing-COVID19/ingest.py",
        dag=dag
    )
    
    dim_to_dwh=BashOperator(
        task_id="dim_to_dwh",
        bash_command="~/spark-3.0.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev --driver-class-path /home/hadoop/postgresql-42.2.6.jar --jars /home/hadoop/postgresql-42.2.6.jar ~/Documents/ETL_Batch_Processing-COVID19/trans_load_dim.py",
        dag=dag
    )
    fact_to_dwh=BashOperator(
        task_id="fact_to_dwh",
        bash_command="~/spark-3.0.3-bin-hadoop3.2/bin/spark-submit --master yarn --queue dev --driver-class-path /home/hadoop/postgresql-42.2.6.jar --jars /home/hadoop/postgresql-42.2.6.jar ~/Documents/ETL_Batch_Processing-COVID19/trans_load_fact.py",
        dag=dag
    )
    stop= DummyOperator(
        task_id="stop"
    )
start>>raw_to_hdfs>>dim_to_dwh>>fact_to_dwh>>stop
