from datetime import datetime as dt
import gspread
import imp
import numpy as np
import pandas as pd
import pickle as pkl
import time

dpath = 'config/data'

class Patcher():
    methods = {0: None,
               1: "get_group_increments",
               2: "get_campaigns_online",
               3: None,
               4: "get_campaigns_offline",
               5: None,
               6: None,
               7: None}

    def __init__(self, df, type, limit, gdf=None):
        """
        Patcher for Google spreadsheets
        -> type: numerical index for spreadsheet
        -> data: dataframe object
        -> limit: max row or column to put new cells
        -> gdf: gspread dataframe (optional)
        """
        self.df = df
        self.type = type
        self.limit = limit
        self.gdf = gdf if isinstance(gdf, pd.DataFrame) else None
        self.d = get_dictionary('data/cell_dicts.pkl')
        self.t = get_dict_value('/triggers.pkl', limit)
        self.patch = self.switch()

    def switch(self):
        """Performs tabwise patch enrichment"""
        return getattr(self, self.methods[self.type])()

    def get_group_increments(self):
        """Incremental test/control weekly, tab 2 on dashboard"""
        patch = []
        for group_type, info in self.d[self.type].items():
            for name, row in info.items():
                x = self.t.increment.get(name)
                patch.append(gspread.models.Cell(row, self.limit, \
                        str(self.df.loc[self.df.group == group_type, x].values[0])))
        return patch

    def get_campaigns_online(self):
        """Online campaigns tab 3 (weekly) or tab 4 (monthly) on dashboard"""
        attr = get_dict_value('/attribution.pkl', self.type)
        def create_new_campaigns(gdf, new_campaigns, max_cols):
            """User interface to update campaign-trigger dictionary
                -> gdf: gspread-dataframe
                -> campaigns: list of unknown campaigns
                -> max_cols: column for current period (row 3)
            """
            def filter_na(df):
                mask = pd.Series(x is not np.nan for x in df.index.values)
                df = df[mask.values].reset_index().append(pd.Series(), ignore_index=True)
                return df.set_index(df.columns[0])

            #Slice google sheet array based on start indices for channels
            outlay = get_dictionary('config/dictionaries/outlay3.pkl')
            main = gdf.iloc[:outlay['email']]
            email = gdf.iloc[outlay['email']:outlay['wp']]
            wp = gdf.iloc[outlay['wp']:outlay['seasonal']]
            seasonal = gdf.iloc[outlay['seasonal']:outlay['sms']]
            sms = filter_na(gdf.iloc[outlay['sms']:])
            del gdf

            def append_campaign(df, channel, trigger):
                """Extends dataframe for each channel's new campaign
                    -> df - dataframe of either email/wp/seasonal/sms
                    -> channel - string representation of channel
                    -> trigger -> new campaign name
                """
                campaign = self.t[channel].get(trigger)
                df = df.append(pd.Series(name = campaign))
                for key, attribution in attr.items():
                    df = df.append(pd.Series(name = attribution))
                df = df.reset_index().append(pd.Series(), ignore_index=True)
                return df.set_index(df.columns[0])

            def get_new_name(name, channel):
                print("Нажмите только 'Enter', если название компании Вас устраивает:\nВведите новое название для отображения в Google Sheets")
                new_name = input()
                if new_name == '':
                    triggers.triggers[channel].update({name:name})
                else:
                    triggers.triggers[channel].update({name:new_name})

            maxlen = len(new_campaigns)
            print("Определите категорию кампании для {n} новых:\n".format(n=maxlen))

            i = 0
            new_names = []
            while(i != maxlen):
                print("Нажмите 'Enter' для Email\n\t'1' для Web-push\n\t'2' для сезонной\n\t'3' для SMS\n")
                v = input('-> ' + new_campaigns[i] + ': \n')
                if v == '':
                    get_new_name(new_campaigns[i], 'email'); i+=1;
                    email = append_campaign(email, 'email', new_campaigns[i-1])
                elif v == '1':
                    get_new_name(new_campaigns[i], 'wp'); i+=1;
                    wp = append_campaign(wp, 'wp', new_campaigns[i-1])
                elif v == '2':
                    get_new_name(new_campaigns[i], 'seasonal'); i+=1;
                    seasonal = append_campaign(seasonal, 'seasonal', new_campaigns[i-1])
                elif v == '3':
                    get_new_name(new_campaigns[i], 'sms'); i+=1;
                    sms = append_campaign(sms, 'sms', new_campaigns[i-1])
                else:
                    print("Повторите ввод в согласии с инструкцией\n")

#update_dictionary(dpath + '/triggers.pkl', self.type, self.t)
            self.gdf = main.append([email, wp, seasonal, sms])
            del main, email, wp, seasonal, sms

            #New channel start indices e.g. web-push row 379
            def reindex_outlay(df):
                """Update start indices for each channel email/wp/seasonal/sms
                -> df: dataframe from gspread get_as_dataframe
                """
                tmp = df.index.values
                for channel in self.t.keys():
                    trigger = list(self.t[channel].values())[0]
                    outlay[channel] = np.where(tmp == trigger)[0][0]
#s.put_dictionary('config/dictionaries/outlay3.pkl', outlay)
            reindex_outlay(self.gdf)

            def update_campaigns(df, gdf, max_cols):
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
                for channel in self.t.keys():
                    for trigger, campaign in self.t[channel].items():
                        campaign_index = gdf[gdf.columns[0]].loc[gdf[gdf.columns[0]] == campaign].index[0]
                        for index, row in gdf.iloc[campaign_index:].iterrows():
                            if pd.isnull(row[0]):
                                break
                            for  attr_mb, attr_gsh in attr.items():
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
                        updates.append(gspread.models.Cell(y_idx+row,
                                                           x_idx+col,
                                                           cell_value))
                return updates

            actual_values = []
            for channel, values in self.t.items():
                for df_trigger in values.keys():
                    actual_values.append(df_trigger)
            new_campaigns = list(self.df.name.loc[~self.df.name.isin(actual_values)])
            create_new_campaigns(self.gdf, new_campaigns, self.limit)
            self.gdf = update_campaigns(self.df, self.gdf, self.limit)
            return build_patch(self.gdf)

    def get_campaigns_offline(self):
        """Offline campaigns weekly, tab 5 on dashboard"""
        attr = get_attribution(self.type)
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
            self.df['date_new'] = self.df.date.apply(lambda x: dt.strftime(x, format='%d.%m'))
            self.df['time'] = self.df.date.apply(lambda x: dt.strftime(x, format='%H:%M'))
            self.df['channel'] = self.df.name.map(get_channel)

        patch = []
        rows = [x for x in range(len(self.df.name.unique()))]
        for ind, name in zip(rows, self.df.name.unique()):
            for key, column in zip(attr.keys(), self.d[self.type].values()):
                patch.append(gspread.models.Cell(self.limit+ind, \
                    column, str(self.df.loc[self.df.name == name, key].values[0])))
        return patch


def put_dictionary(filename, obj):
    with open(filename, 'wb') as f:
        pkl.dump(obj, f, protocol=pkl.HIGHEST_PROTOCOL)

def get_dictionary(filename):
    with open(filename, 'rb') as f:
        d = pkl.load(f)

def update_dictionary(filename, key, value):
    tmp = get_dictionary(filename)
    tmp[key] = value
    put_dictionary(filename, tmp)

def get_dict_value(dict, key):
    return get_dictionary(dpath + dict).get(key)
