# import and install packages
import requests
from datetime import datetime
from dateutil import tz
import pandas as pd

def currentday(base_url, lat, lon, units, api_key, new_tz):
    weather_df = pd.DataFrame()
    today = datetime.now()
    new_time = str(int((today).timestamp()))
    api_call = base_url + "lat=" + lat + "&lon=" + lon + "&dt=" + new_time + "&units=" + units + "&appid=" + api_key

    # get method of requests module
    # return response object
    response = requests.get(api_call)

    # json method of response object
    # convert json format data into
    # python format data
    weather_json = response.json()

    # load data to dataframe and do timezone conversion
    hourly_df = pd.DataFrame(weather_json['hourly'])
    hourly_df['datetime'] = hourly_df['dt'].apply(lambda x: datetime.fromtimestamp(x)
                                                      .astimezone(new_tz)
                                                      .replace(tzinfo=None))
    weather_df = weather_df.append(hourly_df)

    weather_df.sort_values(by='datetime', inplace=True)
    file_name = f'weather_hourly_SA_timestamp_{new_time}.csv'
    # weather_df.to_csv(file_name, index=False)

    return file_name,weather_df

def main():
    # setup parameters check this https://openweathermap.org/api/one-call-api#data
    api_key = '7f0c1694586e146cd38d4d1863743fbd'
    base_url = 'https://api.openweathermap.org/data/2.5/onecall/timemachine?'

    # Harare, South Africa
    lat = str(-17.82772) 
    lon = str(31.05337)
    new_tz = tz.gettz('UTC+2')

    # Temperature is available in Fahrenheit, Celsius and Kelvin units.
    # Wind speed is available in miles/hour and meter/sec.
    # For temperature in Fahrenheit and wind speed in miles/hour, use units=imperial
    # For temperature in Celsius and wind speed in meter/sec, use units=metric
    # Temperature in Kelvin and wind speed in meter/sec is used by default
    units = 'metric'

    file_name, weather_df = currentday(base_url, lat, lon, units, api_key, new_tz)

    return file_name, weather_df[-2:-1]

if __name__ == "__main__":
    main()
