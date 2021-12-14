import pandas as pd
import numpy as np
from datetime import datetime

def main(consumption_data, weather_data, public_holidays_data, service_location_id):
    # Process consumption data
    df = consumption_data.astype({'date':'datetime64[ns]'}).rename(columns={'date':'datetime'}).set_index('datetime')
    df = pd.DataFrame(df['consumption'])
    df = df.asfreq('1H')

    # Convert consumption column to kWH (its a more common metric than Wh)
    df['consumption'] = df['consumption']/1000
    df.rename(columns={'consumption':'consumption_kWh'}, inplace=True)

    # Add season column
    df['date'] = df.index.strftime('%Y-%m-%d')
    df['year'] = df.index.year
    df['dayOfYear'] = df.index.dayofyear
    df['month'] = df.index.month
    df['monthName'] = df.index.month_name()
    df['week'] = df.index.isocalendar().week
    df['day'] = df.index.day
    df['dayName'] = df.index.day_name()
    df['hour'] = df.index.hour
    df['minute'] = df.index.minute
    df['dayOfWeek'] = df.index.dayofweek
    df['weekend'] = df['dayOfWeek'].apply(lambda x: 1 if x >= 5 else 0)
    df['time'] = df.index.time
    df['dayOfMonth'] = df.index.strftime('%m-%d')
    df['hourMinute'] = df.index.strftime('%H:%M')

    bins = [0,4,8,12,16,20,24]
    #labels = ['Late Night', 'Early Morning','Morning','Noon','Eve','Night']
    labels = [1, 2,3,4,5,6]
    df['session'] = pd.cut(df['hour'], bins=bins, labels=labels, include_lowest=True)

    def season_df(df):
        if df['month'] == 12 | df['month'] == 1 | df['month'] == 2:
            return 2 #'Summer'
        elif df['month'] == 3 | df['month'] == 4 | df['month'] == 5:
            return 3 #'Autumn'
        elif df['month'] == 6 | df['month'] == 7 | df['month'] == 8:
            return 4 #'Winter'
        else:
            return 1 #'Spring'

    df['season'] = df.apply(season_df, axis = 1)

    # Process weather data
    weather_df = weather_data.astype({'datetime':'datetime64[ns]'})
    weather_df = weather_df[['temp', 'humidity', 'clouds','datetime']].set_index('datetime')
    weather_df = weather_df.asfreq('1H')

    # Rename and divide by 100 to make it more ML friendly
    weather_df['clouds'] = weather_df['clouds']/100
    weather_df.rename(columns={'clouds':'cloud_cover'}, inplace=True)
    
    # Temperature in degrees C, rename with units
    weather_df.rename(columns={'temp':'temp_degreeC'}, inplace=True)

    # Humidity is relative humidity as a %
    # Rename and divide by 100 to make it more ML friendly
    weather_df['humidity'] = weather_df['humidity']/100
    weather_df.rename(columns={'humidity':'rel_humidity'}, inplace=True)

    # Process holiday data
    holiday_df = public_holidays_data
    holiday_df = holiday_df[['day','holiday','holidayName']]
    holiday_df.rename(columns = {'day':'date'},inplace=True)
    
    # Merge all datasets
    combined_df = df.join(weather_df)

    combined_df['date'] = pd.to_datetime(combined_df['date'], utc = False)
    holiday_df['date'] = pd.to_datetime(holiday_df['date'], utc = False)
    combined_df = pd.merge(combined_df.reset_index(), holiday_df)
    combined_df = combined_df.rename(columns={'index':'datetime'}).set_index('datetime')

    # Replace Holiday 'Y' with 1
    # Replace Holiday NaN with 0
    combined_df['holiday'] = np.where(combined_df['holiday']=='Y',1,0)
    # Add workingday or non-working day column
    combined_df['workingDay'] = np.where(np.logical_and(combined_df['weekend']==0, combined_df['holiday']==0),1,0)

    today = datetime.now()
    new_time = str(int((today).timestamp()))
    file_name = f'merged_{service_location_id}_timestamp_{new_time}.csv'
    return file_name, combined_df
