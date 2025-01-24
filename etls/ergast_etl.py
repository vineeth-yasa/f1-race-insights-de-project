import requests
import numpy as np
import pandas as pd
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2 as pg
from utils.constants import DATABASE_HOST,DATABASE_NAME,DATABASE_PASSWORD,DATABASE_PORT,DATABASE_USER

def insert_processed_date(**kwargs):
    ti=kwargs.get('ti')
    race_date=ti.xcom_pull(key='race_date')
    season=kwargs['params'].get('year')
    round=kwargs['params'].get('race_id')

    conn_postgres=pg.connect(dbname=DATABASE_NAME,user=DATABASE_USER,password=DATABASE_PASSWORD, host=DATABASE_HOST, port=DATABASE_PORT)
    cursor=conn_postgres.cursor()

    cursor.execute(""" CREATE TABLE IF NOT EXISTS processed_races 
                   (race_id SERIAL PRIMARY KEY, race_date DATE NOT NULL UNIQUE, season INTEGER, round INTEGER, last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                   """)
    
    cursor.execute("""  INSERT INTO processed_races (race_date, season, round) 
        VALUES (%s, %s, %s) 
        ON CONFLICT (race_date) 
        DO UPDATE SET 
        last_processed_at = CURRENT_TIMESTAMP; """,  (race_date, season, round) )
    
    conn_postgres.commit()

    cursor.close()
    conn_postgres.close()

def decide_execution_branch(**kwargs):
    round=kwargs.get('params').get('race_id')
    if round==None:
        return 'extract_load_full_season'
    else:
        return 'extract_race_data'


def extract_data(**kwargs):
    year=kwargs['params'].get('year')
    race_id=kwargs['params'].get('race_id')
    ti=kwargs.get('ti')

    url='https://ergast.com/api/f1'
    try:
        req=requests.get(f'{url}/{year}/{race_id}/results.json')
        req_data=req.json()
        if len(req_data['MRData']['RaceTable']['Races'])==0:
            return None
        else:
            race_date=req_data['MRData']['RaceTable']['Races'][0]['date']
            ti.xcom_push(key='race_date',value=race_date)
            
            return req_data
    except Exception as e:
        return e



def load_to_s3(**kwargs):
    bucket_name='f1-season-analysis'
    year=kwargs['params'].get('year')
    race_id=kwargs['params'].get('race_id')
    ti=kwargs['ti']
    race_data=ti.xcom_pull(task_ids='extract_race_data')
    s3_hook= S3Hook(aws_conn_id='aws_default')

    if not s3_hook.check_for_bucket('f1-season-analysis'):
        raise ValueError(f'Bucket {bucket_name} does not exist')
    
    s3_key=f'raw/year={year}/race_{race_id}_results.json'
    race_data_json=json.dumps(race_data)
    
    try:
        s3_hook.load_string(
            string_data=race_data_json,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Data successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        return e
    


    
