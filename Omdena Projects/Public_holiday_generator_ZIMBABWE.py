from datetime import date, datetime
import pandas as pd

def get_holidays(startYear = 2018, endYear = 2025):
    """
    Takes in a start and end date, and start, end year 
    holidays package does not contain ZIMBABWE, which need manually setup
    
    Produces a dataframe with a daily date and columns:
    holiday - 'Y' for holiday
    holidayName - name of the holiday if holiday is 'Y
    
    Returns a dataframe
    """
    
    holidayDict = {u'2018-01-01': 'New Year''s Day',
 u'2018-02-21': 'Robert Mugabe National Youth Day',
 u'2018-03-30': 'Good Friday',
 u'2018-03-31': 'Easter Saturday',
 u'2018-04-01': 'Easter Sunday',
 u'2018-04-02': 'Easter Monday',
 u'2018-04-18': 'Independence Day',
 u'2018-05-01': 'Workder''s Day',
 u'2018-05-25': 'Africa Day',
 u'2018-06-30': 'Polling Day',
 u'2018-08-13': 'Heroes'' Day',
 u'2018-08-14': 'Defense Forces Day',
 u'2018-12-22': 'National Unity Day',
 u'2018-12-25': 'Christmas Day',
 u'2018-12-26': 'Boxing Day',

 u'2019-01-01': 'New Year''s Day',
 u'2019-02-21': 'Robert Mugabe National Youth Day',
 u'2019-04-18': 'Good Friday',
 u'2019-04-19': 'Easter Saturday',
 u'2019-04-20': 'Easter Sunday',
 u'2019-04-21': 'Easter Monday',
 u'2019-04-22': 'Independence Day',
 u'2019-05-01': 'Workder''s Day',
 u'2019-05-25': 'Africa Day',
 u'2019-08-12': 'Heroes'' Day',
 u'2019-08-13': 'Defense Forces Day',
 u'2019-10-25': 'Solidarity Day Against Sanctions',
 u'2019-12-23': 'National Unity Day',
 u'2019-12-25': 'Christmas Day',
 u'2019-12-26': 'Boxing Day',

 u'2020-01-01': 'New Year''s Day',
 u'2020-02-21': 'Robert Mugabe National Youth Day',
 u'2020-04-10': 'Good Friday',
 u'2020-04-11': 'Easter Saturday',
 u'2020-04-12': 'Easter Sunday',
 u'2020-04-13': 'Easter Monday',
 u'2020-04-18': 'Independence Day',
 u'2020-05-01': 'Workder''s Day',
 u'2020-05-25': 'Africa Day',
 u'2020-08-10': 'Heroes'' Day',
 u'2020-08-11': 'Defense Forces Day',
 u'2020-12-22': 'National Unity Day',
 u'2020-12-25': 'Christmas Day',
 u'2020-12-26': 'Boxing Day',

 u'2021-01-01': 'New Year''s Day',
 u'2021-02-21': 'Robert Mugabe National Youth Day',
 u'2021-04-02': 'Good Friday',
 u'2021-04-03': 'Easter Saturday',
 u'2021-04-04': 'Easter Sunday',
 u'2021-04-05': 'Easter Monday',
 u'2021-04-18': 'Independence Day',
 u'2021-05-01': 'Workder''s Day',
 u'2021-05-25': 'Africa Day',
 u'2021-08-09': 'Heroes'' Day',
 u'2021-08-10': 'Defense Forces Day',
 u'2021-12-22': 'National Unity Day',
 u'2021-12-25': 'Christmas Day',
 u'2021-12-26': 'Boxing Day',

 u'2022-01-01': 'New Year''s Day',
 u'2022-02-21': 'Robert Mugabe National Youth Day',
 u'2022-04-15': 'Good Friday',
 u'2022-04-16': 'Easter Saturday',
 u'2022-04-17': 'Easter Sunday',
 u'2022-04-18': 'Easter Monday',
 u'2022-05-01': 'Workder''s Day',
 u'2022-05-25': 'Africa Day',
 u'2022-08-08': 'Heroes'' Day',
 u'2022-08-09': 'Defense Forces Day',
 u'2022-12-22': 'National Unity Day',
 u'2022-12-26': 'Boxing Day',
 u'2022-12-27': 'Christmas Day',

 u'2023-01-02': 'New Year''s Day',
 u'2023-02-21': 'Robert Mugabe National Youth Day',
 u'2023-04-07': 'Good Friday',
 u'2023-04-08': 'Easter Saturday',
 u'2023-04-09': 'Easter Sunday',
 u'2023-04-10': 'Easter Monday',
 u'2023-04-18': 'Independence Day',
 u'2023-05-01': 'Workder''s Day',
 u'2023-05-25': 'Africa Day',
 u'2023-08-14': 'Heroes'' Day',
 u'2023-08-15': 'Defense Forces Day',
 u'2023-12-22': 'National Unity Day',
 u'2023-12-25': 'Christmas Day',
 u'2023-12-26': 'Boxing Day',

 u'2024-01-01': 'New Year''s Day',
 u'2024-02-21': 'Robert Mugabe National Youth Day',
 u'2024-03-29': 'Good Friday',
 u'2024-03-30': 'Easter Saturday',
 u'2024-03-31': 'Easter Sunday',
 u'2024-04-01': 'Easter Monday',
 u'2024-04-18': 'Independence Day',
 u'2024-05-01': 'Workder''s Day',
 u'2024-05-25': 'Africa Day',
 u'2024-08-12': 'Heroes'' Day',
 u'2024-08-13': 'Defense Forces Day',
 u'2024-12-23': 'National Unity Day',
 u'2024-12-25': 'Christmas Day',
 u'2024-12-26': 'Boxing Day',

 u'2025-01-01': 'New Year''s Day',
 u'2025-02-21': 'Robert Mugabe National Youth Day',
 u'2025-04-18': 'Good Friday',
 u'2025-04-19': 'Easter Saturday',
 u'2025-04-20': 'Easter Sunday',
 u'2025-04-21': 'Easter Monday',
 u'2025-05-01': 'Workder''s Day',
 u'2025-05-26': 'Africa Day',
 u'2025-08-11': 'Heroes'' Day',
 u'2025-08-12': 'Defense Forces Day',
 u'2025-12-22': 'National Unity Day',
 u'2025-12-25': 'Christmas Day',
 u'2025-12-26': 'Boxing Day'}
    holiday_df = pd.DataFrame(holidayDict.items(), columns = ['day','holidayName'])
    holiday_df['day'] = pd.to_datetime(holiday_df['day'])
    
    return holiday_df

def get_days(start = '1/1/2018',startYear = 2018, end = '31/12/2025',endYear = 2025):
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
    country_holidays = get_holidays(startYear = startYear, endYear = endYear)
    
    date_df['dayName'] = pd.DatetimeIndex(date_df['day']).day_name()
    date_df['dayOfWeek'] = date_df['day'].dt.dayofweek
    date_df['weekend'] = date_df['dayOfWeek'].apply(lambda x: 'Y' if x>4 else 'N')
    date_df['weekNum'] = date_df['day'].dt.week
    date_df['holiday'] = date_df['day'].apply(lambda x: 'Y' if x in list(country_holidays['day']) else 'N')
    
    date_df = date_df.merge(country_holidays, on='day', how='left', indicator=False)
    
    date_df.to_csv(f'../public_holidays_weekends_ZIMBABWE.csv', index=False)
    return date_df

start = '1/1/2018'
startYear = 2018
end = '31/12/2025'
endYear = 2025
get_days(start = start, startYear = startYear, end = end, endYear = endYear)
