from datetime import datetime as dt
import pandas as pd


def format_data(df):
    """User interface to update campaign-trigger dictionary
        -> gdf: gspread-dataframe
        -> campaigns: list of unknown campaigns
        -> max_cols: column for current period (row 3)
    """
    def get_channel(string):
        string = string.lower()
        if 'sms' in string:
            return 'SMS'
        elif 'web-push' in string or 'wp' in string:
            return 'Web-push'
        else:
            return 'Email'

    pd.reset_option('mode.chained_assignment')
    with pd.option_context('mode.chained_assignment', None):
        df['date_new'] = df.date.apply(lambda x: dt.strftime(x, format='%d.%m'))
        df['time'] = df.date.apply(lambda x: dt.strftime(x, format='%H:%M'))
        df['channel'] = df.name.map(get_channel)

    valsToRound = ['avg_bill']
    valsToPercent = ['open_rate', 'click_rate', 'CTR', \
                     'unfollow_rate', 'order_conversion']

    df[valsToPercent] = df[valsToPercent].astype(str)\
                        .replace('\.', ',', regex=True).apply(lambda x: x+'%')
    df[valsToRound] = df[valsToRound].astype('int64')
    return df
