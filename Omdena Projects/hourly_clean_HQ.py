import pandas as pd
import numpy as np

def main(merged_data):
    df = merged_data.set_index('datetime')
    
    # Drop duplication
    df = df[~df.index.duplicated(keep = 'first')]

    # Impulate missing consumption data using average values for weekend, month and hour
    profile_df = df.groupby([df['weekend'], df['monthName'], df['hour']]).mean()
    for month in df[df['consumption_kWh']==0]['monthName'].unique():
        weekday_avg = profile_df.loc[0, month]['consumption_kWh']
        weekend_avg = profile_df.loc[1, month]['consumption_kWh']
        df[(df['monthName'] == month) & (df['weekend'] == 0) & (df['consumption_kWh'] == 0)] = weekday_avg.values
        df[(df['monthName'] == month) & (df['weekend'] == 1) & (df['consumption_kWh'] == 0)] = weekend_avg.values

    # impulate missing weather data using backward fill
    weather_features = ['temp_degreeC', 'rel_humidity', 'cloud_cover']
    for feature in weather_features:
        df[feature] = df[feature].fillna(method = 'bfill')

    # Use simple forward fill to replace negative cloud cover values
    df['cloud_cover']= df['cloud_cover'].mask(df['cloud_cover']<0).ffill(downcast='infer')

    return df

