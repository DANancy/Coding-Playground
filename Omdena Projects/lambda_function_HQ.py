import json
import hourly_clean_HQ as dcHQ
from io import StringIO # python3; python2: BytesIO 
import boto3
import pandas as pd

def lambda_handler(event, context):
    s3_r = boto3.resource("s3")  # pointer to AWS S3 service
    s3_c = boto3.client('s3')
    bucket_name = "need-energy-dashboard-cleaned-data" 

    csv_buffer = StringIO()
    # get daily merged file
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    merged_obj = s3_c.get_object(Bucket=source_bucket, Key=source_key) 
    merged_df = pd.read_csv(merged_obj['Body'])

    # get historical data
    cleaned_file_name = 'data_hourly_id_47803_cleaned.csv'
    cleaned_obj = s3_c.get_object(Bucket=bucket_name, Key=cleaned_file_name) 
    cleaned_df = pd.read_csv(cleaned_obj['Body'])
   
    # merge daily and historical data
    combined_df = cleaned_df.append(merged_df)
   
    # run clean function to do data wrangling for modelling
    new_cleaned_df = dcHQ.main(combined_df)
    new_cleaned_df.to_csv(csv_buffer)

    # write object to bucket
    s3_r.Object(bucket_name, f'{cleaned_file_name}').put(Body=csv_buffer.getvalue())

    # print result
    return f"Succeed! Find your new {f'{cleaned_file_name}'} file in {bucket_name} bucket.)"

