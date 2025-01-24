from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta
from airflow.utils.trigger_rule import TriggerRule
import sys
import os

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.latest_race_etl import check_last_processed_race, extract_latest_data, decide_execution_branch, insert_processed_date,load_latest_to_s3

default_args={
    'owner': 'vin_yas',
    'email_on_failure': False,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'start_date':datetime(2024,12,15)
}



with DAG (
    dag_id='f1_latest_data_etl',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['f1','latest','etl','pipeline']

) as dag:
    
    check_last_processed_race_date=PythonOperator(
        task_id='check_last_processed_race_date',
        python_callable=check_last_processed_race,
    )

    check_latest_data_availability=BranchPythonOperator(
        task_id='check_latest_data_availability',
        python_callable=decide_execution_branch
    )

    extract_latest_race_data=PythonOperator(
        task_id='extract_latest_race_data',
        python_callable=extract_latest_data
    )

    load_latest_results_to_s3=PythonOperator(
        task_id='load_latest_results_to_s3',
        python_callable=load_latest_to_s3
    )

    save_latest_race_date=PythonOperator(
        task_id='save_latest_race_date',
        python_callable=insert_processed_date
    )

    no_new_races_to_process= EmptyOperator(task_id='no_new_races_to_process')

    end_pipeline= EmptyOperator(
        task_id='end_pipeline',
        trigger_rule=TriggerRule.ONE_SUCCESS                       
    )


    check_last_processed_race_date >> check_latest_data_availability >> [extract_latest_race_data,no_new_races_to_process]

    extract_latest_race_data >> load_latest_results_to_s3 >> save_latest_race_date >> end_pipeline

    no_new_races_to_process>> end_pipeline