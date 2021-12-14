from datetime import datetime
import requests
import pandas as pd
import json
from datetime import datetime,timedelta 
from dateutil import tz

HERE_TZ = tz.tzlocal()
ZIMBABWE_TZ = tz.gettz('UTC+2')
COLUMNS = ['date', 'timestamp', 'consumption', 'solar', 'alwaysOn', 'gridImport',
       'gridExport', 'selfConsumption', 'selfSufficiency', 'active',
       'reactive', 'voltages', 'phaseVoltages', 'currentHarmonics',
       'voltageHarmonics']
AGGREGATION_CODES = {"five_min":"1", "hourly":"2", "daily":"3",
                     "monthly":"4", "quarterly":"5", "ten_min":"6",
                     "fifteen_min":"7", "twenty_min":"8", "thirty":"9"}


def collect_data(aggregation_type, service_location_id,new_tz):
    # Setting up variables
    aggregation_code = AGGREGATION_CODES[aggregation_type]

    from_ = int((datetime.now() - timedelta(hours=1)).replace(minute=0, second=0).timestamp()) * 1000
    to_ = int((datetime.now()).replace(minute=0, second=0).timestamp()) * 1000  
    
    # Making request
    URL = "https://app1pub.smappee.net/dev/v3/servicelocation/{}/consumption?aggregation={}&from={}&to={}"
    url = URL.format(service_location_id, aggregation_code, from_, to_)
    payload={}
    headers = {
      'Authorization': 'Bearer ab299760-c43d-320c-8be0-1bb74a643a8b'
    }
    response = requests.request("GET", url, headers=headers, data=payload)

    # Json Serializing
    data = json.loads(response.text)

    # Converting to pandas and saving .csv file
    try:
        data_df = pd.DataFrame(data['consumptions'])
        data_df['date'] = data_df['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000)
                                                .astimezone(new_tz)
                                                .replace(tzinfo=None))
        data_df = data_df[COLUMNS]
        file_name = f'data_{aggregation_type}_id_{service_location_id}_timestamp_{from_}.csv'
        # data_df.to_csv(file_name, index=False)
    except KeyError as e:
        columns = ['date','timestamp','consumption','solar','alwaysOn','gridImport','gridExport','selfConsumption','selfSufficiency','active','reactive','voltages','phaseVoltages','currentHarmonics','voltageHarmonics']
        data_df = pd.DataFrame(columns = columns)
        file_name = f'data_{aggregation_type}_id_{service_location_id}_timestamp_{from_}.csv'
        print('I got a KeyError - reason "%s"' % str(e))
    return file_name, data_df

def main():
    # aggregation_types = ['five_min', 'hourly', 'daily', 'monthly']
    aggregation_types = ['hourly']
    service_location_ids = [
    '47740',# Puma Rhodesville,
    '47803'# Puma HQ# #
    ]
    new_tz = tz.gettz('UTC+2')
    
    file_names = []
    data_dfs = []
    for service_location_id in service_location_ids:
        for aggregation_type in aggregation_types:
            file_name, data_df = collect_data(aggregation_type=aggregation_type, service_location_id=service_location_id,new_tz=new_tz)
            file_names.append(file_name)
            data_dfs.append(data_df)
    return service_location_ids, file_names, data_dfs

if __name__ == "__main__":
    main()
