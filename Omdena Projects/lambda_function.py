import boto3
import pandas as pd
import hourly_consumption as dc
import hourly_weather as dw
import hourly_merge as dm
from io import StringIO # python3; python2: BytesIO 

def lambda_handler(event, context):
    s3_r = boto3.resource("s3")  # pointer to AWS S3 service
    s3_c = boto3.client('s3')

    bucket_name = "need-energy-dashboard-raw-data" # bucket for saving our data
    bucket_to = "need-energy-dashboard-merged-data"
    
    # run daily api to generate daily consumption and save historical merged consumption data to merged 
    service_location_ids, consumption_files, consumption_dfs = dc.main()
    for i in range(0,2):
        consumption_file = consumption_files[i]
        consumption_df = consumption_dfs[i]
        service_location_id =  service_location_ids[i]

        # consumption_buffer = StringIO()
        # consumption_df.to_csv(consumption_buffer, date_format="%Y-%m-%d %H:%M:%S")
        # write object to bucket
        # s3_r.Object(bucket_name, f'consumption/{consumption_file}').put(Body=consumption_buffer.getvalue())
        
        histical_file_name = f'data_hourly_id_{service_location_id}.csv'
        histical_file_obj = s3_c.get_object(Bucket=bucket_to, Key=histical_file_name) 
        histical_file_df = pd.read_csv(histical_file_obj['Body'])
        histical_file_df = histical_file_df.append(consumption_df)
        cols_to_keep = ['date','timestamp','consumption','solar','alwaysOn','gridImport','gridExport','selfConsumption','selfSufficiency','active','reactive','voltages','phaseVoltages','currentHarmonics','voltageHarmonics']
        histical_buffer = StringIO()
        histical_file_df.loc[:, cols_to_keep].to_csv(histical_buffer, date_format="%Y-%m-%d %H:%M:%S")
        # write object to bucket
        s3_r.Object(bucket_to, f'{histical_file_name}').put(Body=histical_buffer.getvalue())
        
    # run daily api to generate daily weather 
    weather_buffer = StringIO()
    weather_file, weather_df = dw.main()
    weather_df.to_csv(weather_buffer,date_format="%Y-%m-%d %H:%M:%S")
    # write object to bucket
    s3_r.Object(bucket_name, f'weather/{weather_file}').put(Body=weather_buffer.getvalue())
    
    # get public holiday object and file (key) from bucket
    SA_holiday_file_name = 'public-holiday/public_holidays_weekends_SA_1994-2025.csv'
    SA_public_holidays_obj = s3_c.get_object(Bucket=bucket_name, Key=SA_holiday_file_name) 
    SA_public_holidays_df = pd.read_csv(SA_public_holidays_obj['Body'])
    
    ZA_holiday_file_name = 'public-holiday/public_holidays_weekends_ZIMBABWE_2018_2025.csv'
    ZA_public_holidays_obj = s3_c.get_object(Bucket=bucket_name, Key=ZA_holiday_file_name) 
    ZA_public_holidays_df = pd.read_csv(ZA_public_holidays_obj['Body'])
    
    # run merge function to merge all datasets
    for i in range(0,2):
        consumption_file = consumption_files[i]
        consumption_df = consumption_dfs[i]
        service_location_id =  service_location_ids[i]
        merged_buffer = StringIO()
        if service_location_id == '47803':
            merged_file, merged_df = dm.main(consumption_df, weather_df,SA_public_holidays_df, service_location_id)
            merged_df.to_csv(merged_buffer,date_format="%Y-%m-%d %H:%M:%S")
            s3_r.Object(bucket_to, f'{merged_file}').put(Body=merged_buffer.getvalue())
        
    # print results
    return f"Succeed!"
