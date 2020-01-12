from patcher import dpath
from gspread.models import Cell
import numpy as np
import pandas as pd
import pickle as pkl

attribution = get_dict_value('/attribution.pkl', 3)

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
    new_names = []
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

#New channel start indices e.g. web-push row 379
def reindex_outlay(df, triggers):
    """Update start indices for each channel email/wp/seasonal/sms
    -> df: dataframe from gspread get_as_dataframe
    """
    tmp = df.index.values
    for channel in triggers.keys():
        trigger = list(triggers[channel].values())[0]
        outlay[channel] = np.where(tmp == trigger)[0][0]
    s.put_dictionary('config/dictionaries/outlay3.pkl', outlay)

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
            campaign_index = gdf[gdf.columns[0]].loc[gdf[gdf.columns[0]] == campaign].index[0]
            for index, row in gdf.iloc[campaign_index:].iterrows():
                if pd.isnull(row[0]):
                    break
                for  attr_mb, attr_gsh in attribution.items():
                    if attr_gsh == row[0]:
                        gdf.iloc[index, max_cols] = get_campaign_info(df, trigger, attr_mb)
    return gdf

def build_patch(df):
    """Returns df as a list of cells to patch for gspread update"""
    row=1
    col=1
    y, x = df.shape
    updates = []
    values = []
    for value_row in df.values:
       values.append(value_row)
    for y_idx, value_row in enumerate(values):
       for x_idx, cell_value in enumerate(value_row):
           updates.append(Cell(y_idx+row, x_idx+col, cell_value))
    return updates
