import requests
import numpy as np
import pandas as pd
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import psycopg2 as pg
import time
from utils.constants import DATABASE_HOST,DATABASE_NAME,DATABASE_PASSWORD,DATABASE_PORT,DATABASE_USER

def full_season_load_pipeline(**kwargs):
    ti= kwargs.get('ti')
    year=kwargs.get('params').get('year')
    round=1
    url='https://ergast.com/api/f1'

    conn_postgres=pg.connect(dbname=DATABASE_NAME,user=DATABASE_USER,password=DATABASE_PASSWORD, host=DATABASE_HOST, port=DATABASE_PORT)
    cursor=conn_postgres.cursor()
    s3_hook= S3Hook(aws_conn_id='aws_default')

    if not s3_hook.check_for_bucket('f1-season-analysis'):
        raise ValueError(f'Bucket {bucket_name} does not exist')
    
    while round!=0:       
        try:

            req=requests.get(f'{url}/{year}/{round}/results.json')
            req_data=req.json()
            if len(req_data['MRData']['RaceTable']['Races'])==0:
                break
            else:
                race_date=req_data['MRData']['RaceTable']['Races'][0]['date']
            
                bucket_name='f1-season-analysis'
                s3_key=f'raw/year={year}/race_{round}_results.json'
                race_data_json=json.dumps(req_data)
    
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

                cursor.execute(""" CREATE TABLE IF NOT EXISTS processed_races 
                   (race_id SERIAL PRIMARY KEY, race_date DATE NOT NULL UNIQUE, season INTEGER, round INTEGER, last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                   """)
    
                cursor.execute("""  INSERT INTO processed_races (race_date, season, round) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (race_date) 
                    DO UPDATE SET 
                    last_processed_at = CURRENT_TIMESTAMP; """,  (race_date, year, round) )
    
                conn_postgres.commit()


                round+=1

                time.sleep(5)
   
        except Exception as e:
            return e
        

    cursor.close()
    conn_postgres.close()

    print(f'Loaded complete {year} season data to S3 ')    