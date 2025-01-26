import json
import boto3
import awswrangler as wr
import pandas as pd
import urllib.parse

s3Client = boto3.client('s3')


def parse_race_results(results):
    results_parsed = []
    for result in results:
        result_parsed={
            'position':result.get('position',None),
            'position_text':result.get('positionText',None),
            'driver_number':result.get('Driver',{}).get('permanentNumber',None),
            'driver_id':result.get('Driver',{}).get('driverId',None),
            'driver_name':result.get('Driver',{}).get('givenName',None)+' '+result.get('Driver',{}).get('familyName',None),
            'driver_nationality':result.get('Driver',{}).get('Nationality',None),
            'constructor_id':result.get('Constructor',{}).get('constructorId',None),
            'constructor_name':result.get('Constructor',{}).get('name',None),
            'grid_start_pos':result.get('grid',None),
            'fastest_lap_rank':result.get('FastestLap', {}).get('rank', None),
            'points':result.get('points',None)
        }
        results_parsed.append(result_parsed)

    return results_parsed

def lambda_handler(event, context):
    
    try: 
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        key=urllib.parse.unquote(key)
        object=s3Client.get_object(Bucket=bucket, Key=key)
        print('Object retrieved')
    except Exception as e:
        print(f'failed to fetch object s3://{bucket}/{key}')
        return str(e)

    object_dict=json.loads(object['Body'].read().decode('utf-8'))

    season=object_dict['MRData']['RaceTable']['season']
    round_no=object_dict['MRData']['RaceTable']['round']
    race_name=object_dict['MRData']['RaceTable']['Races'][0]['raceName']
    race_date=object_dict['MRData']['RaceTable']['Races'][0]['date']
    race_results=object_dict['MRData']['RaceTable']['Races'][0]['Results']

    parsed_results = parse_race_results(race_results) 

    parsed_results_df = pd.DataFrame(parsed_results)

    parsed_results_df['season'] = season
    parsed_results_df['round'] = round_no
    parsed_results_df['race_name'] = race_name
    parsed_results_df['race_date'] = race_date
    parsed_results_df['fastest_lap_rank']=parsed_results_df['fastest_lap_rank'].astype(str)

    #print(parsed_results_df.head())
    try:
        wr.s3.to_parquet(
            df=parsed_results_df,
            path=f's3://{bucket}/processed/year={season}/race_{round_no}_results.parquet',
            index=False,
            dataset=False
            
        )
        return {
            "statusCode": 200,
            "body": "Processed raw results JSON and saved to S3!"
        }

    except Exception as e:
        print('Failed to save results to S3:', str(e))

        # Return error response as a JSON-compatible object
        return {
            "statusCode": 500,
            "body": {
                "message": "Failed to save results to S3",
                "error": str(e)
            }
        }
    
