from datetime import date, datetime
import pandas as pd
import holidays
def get_holidays(startYear = 2018, endYear = 2025, countryCode = 'ZA'):
    """
    Takes in a start and end date, and start and end year.
    
    Produces a dataframe with a daily date and columns:
    holiday - 'Y' for holiday
    holidayName - name of the holiday if holiday is 'Y'
    
    Returns a dataframe
    """
    
    holidayDict = {}
    for i in range(startYear, endYear):
        for date, name in sorted(holidays.CountryHoliday(countryCode,years=[i]).items()):
            holidayDict[date] = name
        holiday_df = pd.DataFrame(list(holidayDict.items()),columns = ['day','holidayName'])
        holiday_df['day'] = pd.to_datetime(holiday_df['day']).dt.date
    return holiday_df

def get_days(start = '1/1/2018',startYear = 2018, end = '31/12/2025',endYear = 2025, countryCode = 'ZA'):
    """
    Takes in a start and end date, and start and end year.
    
    Produces a dataframe with a daily date and columns:
    weekend - 'Y' for weendend and 'N' for workday
    dayOfweek - numerical day of the week identifier 0 for monday
    weekNum - numerical number of the week 
    holiday - 'Y' for holiday
    holidayName - name of the holiday if holiday is 'Y'
    
    Returns a dataframe
    """
    
    #generate the range of daily dates
    dates = pd.date_range(start = start, end = end)
    date_df = pd.DataFrame(dates, columns = ['day'])
    date_df['day'] = pd.to_datetime(date_df['day'])
    country_holidays = get_holidays(startYear = startYear, endYear = endYear, countryCode = countryCode)
    
    date_df['dayName'] = pd.DatetimeIndex(date_df['day']).day_name()
    date_df['dayOfWeek'] = date_df['day'].dt.dayofweek
    date_df['weekend'] = date_df['dayOfWeek'].apply(lambda x: 'Y' if x>4 else 'N')
    date_df['weekNum'] = date_df['day'].dt.week
    date_df['holiday'] = date_df['day'].apply(lambda x: 'Y' if x in country_holidays['day'].values else 'N')
    date_df['day'] = date_df['day'].dt.date
    date_df = date_df.merge(country_holidays, on='day', how='left', indicator=False)
    
    date_df.to_csv(f'../public_holidays_weekends.csv', index=False)
    return date_df

start = '1/1/1994'
startYear = 1994
end = '31/12/2021'
endYear = 2021
countryCode = 'ZA'
get_days(start = start, startYear = startYear, end = end, endYear = endYear, countryCode = countryCode)
