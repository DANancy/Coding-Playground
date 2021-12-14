import pandas as pd
import solcast
from dateutil import tz
from datetime import datetime

API_KEY = '8bQiJkGE--ukTUA2l20--y1S3QYa1wEg'
latitude = -17.82772
longitude = 31.05337
new_tz = tz.gettz('UTC+2')

def main():
    today = datetime.now()
    new_time = str(int((today).timestamp()))

    data = solcast.get_radiation_forecasts(latitude, longitude, API_KEY)
    seven_day_forecast = data.forecasts
    data_df = pd.DataFrame(seven_day_forecast)
    data_df['period_end'] = data_df['period_end'].apply(lambda x: x.astimezone(new_tz).replace(tzinfo=None))

    file_name = f'solar_data_7_days_forecast.csv'
    return file_name, data_df
    
if __name__ == "__main__":
    main()

