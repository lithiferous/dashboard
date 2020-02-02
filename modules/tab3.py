from collections import namedtuple as nt
from gspread_formatting import *
from gspread.utils import rowcol_to_a1
import numpy as np
import pandas as pd
import pickle as pkl

dpath = 'config/data/'


def get_dict(filename):
    with open(filename, 'rb') as f:
        return pkl.load(f)


def get_dict_value(filename, key):
    return get_dict(dpath + filename).get(key)


attribution = get_dict_value('attribution.pkl', 2)
outlay = get_dict(dpath + 'outlay3.pkl')


def filter_na(df):
    mask = pd.Series(x is not np.nan for x in df.index.values)
    df = df[mask.values].reset_index().append(pd.Series(), ignore_index=True)
    return df.set_index(df.columns[0])


def append_campaign(df, channel, trigger, triggers, attribution):
    """Extends dataframe for each channel's new campaign
        -> df - dataframe of either email/wp/seasonal/sms
        -> channel - string representation of channel
        -> trigger -> new campaign name
    """
    campaign = triggers[channel].get(trigger)
    df = df.append(pd.Series(name = campaign))
    for key, attr in attribution.items():
        df = df.append(pd.Series(name = attr))
    df = df.reset_index().append(pd.Series(), ignore_index=True)
    return df.set_index(df.columns[0])


def get_new_name(name, channel, triggers):
    print("Нажмите только 'Enter', если название компании Вас устраивает:\nВведите новое название для отображения в Google Sheets")
    new_name = input()
    if new_name == '':
        triggers[channel].update({name:name})
    else:
        triggers[channel].update({name:new_name})


def check_new_groups(new_campaigns,
                     main, email, wp,
                     seasonal, sms,
                     triggers, type_):
    maxlen = len(new_campaigns)
    print("Определите категорию кампании для {n} новых:\n".format(n=maxlen))

    i = 0
    while(i != maxlen):
        print("Нажмите 'Enter' для Email\n\t'1' для Web-push\n\t'2' для сезонной\n\t'3' для SMS\n")
        v = input('-> ' + new_campaigns[i] + ': \n')
        if v == '':
            get_new_name(new_campaigns[i], 'email', triggers); i+=1;
            email = append_campaign(email, 'email', new_campaigns[i-1],
                                    triggers, attribution)
        elif v == '1':
            get_new_name(new_campaigns[i], 'wp', triggers); i+=1;
            wp = append_campaign(wp, 'wp', new_campaigns[i-1],
                                 triggers, attribution)
        elif v == '2':
            get_new_name(new_campaigns[i], 'seasonal', triggers); i+=1;
            seasonal = append_campaign(seasonal, 'seasonal', new_campaigns[i-1],
                                       triggers, attribution)
        elif v == '3':
            get_new_name(new_campaigns[i], 'sms', triggers); i+=1;
            sms = append_campaign(sms, 'sms', new_campaigns[i-1],
                                  triggers, attribution)
        else:
            print("Повторите ввод в согласии с инструкцией\n")
    #update_dictionary(dpath + '/triggers.pkl', type_, triggers)
    return main.append([email, wp, seasonal, sms])


def reindex_outlay(df, triggers):
    """Update start indices for each channel email/wp/seasonal/sms
    -> df: dataframe from gspread get_as_dataframe
    """
    tmp = df.index.values
    for channel in triggers.keys():
        trigger = list(triggers[channel].values())[0]
        outlay[channel] = np.where(tmp == trigger)[0][0]
    #put_dictionary(dpath+'outlay3.pkl', outlay)


def update_campaigns(df, gdf, max_cols,
                     triggers, attribution):
    """Assigns new campaign's attribution values to dataframe
    -> df: dataframe from MindBox report
    -> gdf: dataframe gspread get_as_dataframe
    -> max_cols: period column
    """
    def get_campaign_info(df, name, column):
        """Locates specific value for named campaign"""
        try: res = df.loc[df.name == name, column].values[0]
        except: res = 0
        return res

    gdf = gdf.reset_index()
    for channel in triggers.keys():
        for trigger, campaign in triggers[channel].items():
            try:
                campaign_index = gdf[gdf.columns[0]].loc[gdf[gdf.columns[0]] == campaign].index[0]
                for index, row in gdf.iloc[campaign_index:].iterrows():
                    if pd.isnull(row[0]):
                        break
                    for attr_mb, attr_gsh in attribution.items():
                        if attr_gsh == row[0]:
                            gdf.iloc[index, max_cols] = get_campaign_info(df, trigger, attr_mb)
            except:
                continue
    return gdf


def get_attr_sum(gdf, channel, max_cols, attr):
    outlay = get_dict(dpath + 'outlay3.pkl')
    df = pd.DataFrame()
    if channel == 'Email':
        df = gdf.iloc[outlay['email']:outlay['wp']]
    elif channel == 'Web-push':
        df = gdf.iloc[outlay['wp']:outlay['seasonal']]
    elif channel == 'Сезонные триггеры':
        df = gdf.iloc[outlay['seasonal']:outlay['sms']]
    indices = df.iloc[:, 0].loc[df.iloc[:, 0].where(df.iloc[:, 0] == attr).notna()].index.values
    return sum(gdf.iloc[indices, max_cols])


class mainStats():
    def __init__(self, gdf, max_cols, channel, traffic):
        self.delivered = get_attr_sum(gdf, channel, max_cols, 'Доставлено')
        self.opened = get_attr_sum(gdf, channel, max_cols, 'Открытия')
        self.clicked = get_attr_sum(gdf, channel, max_cols, 'Кликов')
        self.revenue = get_attr_sum(gdf, channel, max_cols, 'Доход по атрибуции MB')
        self.orders = get_attr_sum(gdf, channel, max_cols, 'Покупок по атрибуции MB')
        self.openr = self.opened/self.delivered
        self.clickr = self.clicked/self.opened
        self.ord_traffic = self.orders/traffic
        self.ord_conversion = self.orders/self.delivered

    def switch(self, attr):
        d = {'Доставлено':         self.delivered,
             'OR':                 self.openr,
             'CR':                 self.clickr,
             'Выручка':            self.revenue,
             'Заказов':            self.orders,
             'Заказов от трафика': self.ord_traffic,
             'Конверсия в заказы': self.ord_conversion}
        return d[attr]


def fill_main(gdf, max_cols):
    """
    -> gdf: dataframe of main table
    """
    channels = ['Email', 'Web-push', 'Сезонные триггеры']

    def get_value_index(series, value):
        series = series.where(series == value)
        return series.loc[series.notnull()].index[0]

    for channel in channels:
        idx = get_value_index(gdf.iloc[:, 0], channel)+1
        stats = mainStats(gdf, max_cols, channel, 1)
        for ind, row in gdf.loc[idx:].iterrows():
            if pd.isnull(row[0]):
                break
            gdf.iloc[ind, max_cols] = stats.switch(row[0])
    return gdf

def build_format_patch(df, limit):
    """Returns new formats for gspread worksheet"""
    def create_format_dict(*args):
        fmt = nt('fmt', ['bold',
                         'italic',
                         'alignment',
                         'background'])
        return fmt(*args)

    main = create_format_dict(True, False, 'left', color(244/255, 204/255, 204/255))
    email = create_format_dict(True, True, 'left', color(255/255, 242/255, 204/255))
    wp = create_format_dict(True, True, 'left', color(208/255, 224/255, 227/255))
    seasonal = create_format_dict(True, True, 'left', color(217/255, 210/255, 233/255))
    sms = create_format_dict(True, True, 'left', color(201/255, 218/255, 248/255))

    main_idx = ['Email', 'Web-push', 'Сезонные триггеры']
    email_idx = get_dict_value('triggers.pkl', 2)['email'].values()
    wp_idx = get_dict_value('triggers.pkl', 2)['wp'].values()
    seasonal_idx = get_dict_value('triggers.pkl', 2)['seasonal'].values()
    sms_idx = get_dict_value('triggers.pkl', 2)['sms'].values()

    formats = [(main,     main_idx),
               (email,    email_idx),
               (wp,       wp_idx),
               (seasonal, seasonal_idx),
               (sms,      sms_idx),]

    def get_format_patch(fmt_, df, limit, ranges):
        def get_cell_format(_fmt):
            return cellFormat(backgroundColor=_fmt.background,
                   textFormat=textFormat(bold=_fmt.bold, italic=_fmt.italic),
                              horizontalAlignment=_fmt.alignment.upper())

        def get_fmt_ranges(df, row_vals, max_cols):
            def get_rows_to_a1(df, values, col):
                def locate_formats(series, row_val):
                    print(row_val)
                    return np.where(series == row_val)[0][0] + 2
                indices = [locate_formats(df.iloc[:, 0], x) for x in values]
                return [rowcol_to_a1(x, col) for x in indices]

            minr = get_rows_to_a1(df, row_vals, 1)
            maxr = get_rows_to_a1(df, row_vals, max_cols)
            ranges = []
            for x, y in zip(minr, maxr):
                ranges.append(x + ':' + y)
            return ranges

        (key, val) = fmt_
        _fmt = get_cell_format(key)
        new_ranges = get_fmt_ranges(df, val, limit)
        for range in new_ranges:
            ranges.append((range, _fmt))
    ranges = []
    for fmt in formats:
        get_format_patch(fmt, df, limit, ranges)
    return ranges
