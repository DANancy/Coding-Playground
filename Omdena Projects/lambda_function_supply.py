import json
import daily_supply as ds
import boto3
from io import StringIO # python3; python2: BytesIO 

def lambda_handler(event, context):
    s3 = boto3.resource("s3")  # pointer to AWS S3 service
    bucket_name = "need-energy-dashboard-cleaned-data" # bucket for saving our data
    
    # run api daily to generate 7 days forecast solar irradiance data
    supply_buffer = StringIO()
    supply_file, supply_df = ds.main()
    supply_df.to_csv(supply_buffer)
    # write object to bucket
    s3.Object(bucket_name, f'{supply_file}').put(Body=supply_buffer.getvalue())

    # print results
    return f"Succeed! Find your new {f'{supply_file}'} file in {bucket_name} bucket."
