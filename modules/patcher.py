from datetime import datetime as dt
import pandas as pd
import pickle as pkl
import gspread
import imp
import time

triggers = imp.load_source('triggers', 'config/triggers.py')

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
        self.t = triggers
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

        online_attr = get_dictionary('config/dictionaries/online_attr.pkl')

        def create_new_campaigns(gdf, new_campaigns, max_cols):
            """User interface to update campaign-trigger dictionary
                -> gdf: gspread-dataframe
                -> campaigns: list of unknown campaigns
                -> max_cols: column for current period (row 3)
            """
            #Slice google sheet array based on start indices for channels
            outlay = get_dictionary('config/dictionaries/outlay3.pkl')
            main = gdf.iloc[:outlay['email']]
            email = gdf.iloc[outlay['email']:outlay['wp']]
            wp = gdf.iloc[outlay['wp']:outlay['seasonal']]
            seasonal = gdf.iloc[outlay['seasonal']:outlay['sms']]
            sms = gdf.iloc[outlay['sms']:]
            del gdf

            def append_campaign(df, channel, trigger):
                """Extends dataframe for each channel's new campaign
                    -> df - dataframe of either email/wp/seasonal/sms
                    -> channel - string representation of channel
                    -> trigger -> new campaign name
                """
                campaign = triggers.triggers[channel].get(trigger)
                df = df.append(pd.Series(name = campaign))
                for key, attribution in online_attr.items():
                    if channel != 'sms' and key == 'sent':
                        df = df.append(pd.Series(name = attribution), \
                                       ignore_index=True)
                df = df.append(pd.Series(), ignore_index=True)

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
                    append_campaign(email, 'email', new_campaigns[i-1])
                elif v == '1':
                    get_new_name(new_campaigns[i], 'wp'); i+=1;
                    append_campaign(wp, 'wp', new_campaigns[i-1])
                elif v == '2':
                    get_new_name(new_campaigns[i], 'seasonal'); i+=1;
                    append_campaign(seasonal, 'seasonal', new_campaigns[i-1])
                elif v == '3':
                    get_new_name(new_campaigns[i], 'sms'); i+=1;
                    append_campaign(sms, 'sms', new_campaigns[i-1])
                else:
                    print("Повторите ввод в согласии с инструкцией\n")

            #Merge dictionaries
            df = main.append([email, wp, seasonal, sms], ignore_index=True)
            del main, email, wp, seasonal, sms

            #New channel start indices e.g. web-push row 379
            def reindex_outlay(df):
                """Update start indices for each channel email/wp/seasonal/sms
                -> df: dataframe from gspread get_as_dataframe
                """
                for channel in triggers.triggers.keys():
                    trigger = list(triggers.triggers[channel].values())[0]
                    outlay[channel] = df[df.columns[0]][df[df.columns[0]] == trigger].index[0]
                put_dictionary('config/dictionaries/outlay3.pkl', outlay)
            reindex_outlay(df)
            return df

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

            for channel in triggers.triggers.keys():
                for trigger, campaign in triggers.triggers[channel].items():
                    campaign_index = gdf[gdf.columns[0]].loc[gdf[gdf.columns[0]] == campaign].index[0]
                    for index, row in gdf.iloc[campaign_index:].iterrows():
                        if pd.isnull(row[0]):
                            break
                        for attrib_mb, attrib_gsh in online_attr.items():
                            if row[0] == attrib_gsh:
                                cell_value = get_campaign_info(df, trigger, attrib_mb)
                                gdf.iloc[index, max_cols] = cell_value

        def build_patch(df):
            """Returns df as a list of cells to patch for gspread update"""
            y, x = df.shape
            updates = []
            values = []
            for value_row in df.values:
                values.append(value_row)
            for y_idx, value_row in enumerate(values):
                for x_idx, cell_value in enumerate(value_row):
                    updates.append((y_idx+row,
                                    x_idx+col,
                                    cell_value))
            return updates
        actual_values = []
        for channel, values in self.t.triggers.items():
            for df_trigger in values.keys():
                actual_values.append(df_trigger)
        new_campaigns = list(self.df.name.loc[~self.df.name.isin(actual_values)])
        self.gdf = create_new_campaigns(self.gdf, new_campaigns, self.limit - 1)
        update_campaigns(self.df, self.gdf, self.limit - 1)
        return build_patch(self.gdf)

    def get_campaigns_offline(self):
        """Offline campaigns weekly, tab 5 on dashboard"""
        self.df = self.df[(self.df.channel=='Ручные рассылки') & \
                          (self.df.sent != 0)]

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
            for key, column in zip(self.t.offline_attr.keys(), self.d[self.type].values()):
                patch.append(gspread.models.Cell(self.limit+ind, \
                    column, str(self.df.loc[self.df.name == name, key].values[0])))
        return patch


def put_dictionary(filename, obj):
    with open(filename, 'wb') as f:
        pkl.dump(obj, f, protocol=pkl.HIGHEST_PROTOCOL)

def get_dictionary(filename):
    with open(filename, 'rb') as f:
        d = pkl.load(f)
    return d
