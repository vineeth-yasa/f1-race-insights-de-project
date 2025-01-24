
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta
from airflow.utils.trigger_rule import TriggerRule
import os
import sys

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etls.ergast_etl import extract_data, load_to_s3, insert_processed_date, decide_execution_branch
from etls.full_season_load import full_season_load_pipeline


default_args={
    'owner': 'vin_yas',
    'email_on_failure': False,
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2024,12,15),
    'catchup': False
    

}

#file_postfix = datetime.now().strftime("%Y%m%d")

with DAG(
    dag_id='f1_extract_load_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['f1','etl','pipeline'],
    params={
        'year':'2024',
        'race_id': None
    }
) as dags:
    
    start_pipeline=EmptyOperator(
        task_id='start_pipeline'
    )

    decide_load_type= BranchPythonOperator(
        task_id='decide_load_type',
        python_callable=decide_execution_branch
    )
    

    extract_load_full_season=PythonOperator(
        task_id='extract_load_full_season',
        python_callable=full_season_load_pipeline
    )



    extract_race_data= PythonOperator(
        task_id='extract_race_data',
        python_callable=extract_data
    )

    load_results_to_s3 = PythonOperator(
        task_id='load_results_to_s3',
        python_callable=load_to_s3,
    )

    track_processed_date= PythonOperator(
        task_id='track_processed_date',
        python_callable=insert_processed_date
    )

    end_pipeline = EmptyOperator(
        task_id= 'end_pipeline',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )


    start_pipeline >> decide_load_type >> [extract_race_data, extract_load_full_season] 
    
    extract_race_data >> load_results_to_s3 >> track_processed_date >> end_pipeline

    extract_load_full_season >> end_pipeline