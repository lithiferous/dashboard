from datetime import datetime as dt
import gspread
import imp
import pandas as pd
import pickle as pkl
import re

dpath = 'config/data/'
tab3 = imp.load_source('online_campaigns', 'modules/tab3.py')


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
        self.format = None
        self.d = get_dict('data/cell_dicts.pkl')
        self.t = get_dict_value('triggers.pkl', type)
        self.patch = self.switch()

    def switch(self):
        """Performs tabwise patch enrichment"""
        return getattr(self, self.methods[self.type])()

    def get_group_increments(self):
        """Incremental test/control weekly, tab 2 on dashboard"""
        patch = []
        for group_type, info in self.d[self.type].items():
            for name, row in info.items():
                x = self.t.get(name)
                patch.append(gspread.models.Cell(row, self.limit,
                        str(self.df.loc[self.df.group == group_type, x].values[0])))
        return patch

    def get_campaigns_online(self):
        """Online campaigns tab 3 (weekly) or tab 4 (monthly) on dashboard"""
        attr = get_dict_value('attribution.pkl', self.type)

        def create_new_campaigns(gdf, new_campaigns, max_cols):
            """User interface to update campaign-trigger dictionary
                -> gdf: gspread-dataframe
                -> campaigns: list of unknown campaigns
                -> max_cols: column for current period (row 3)
            """
            # Slice google sheet array based on start indices for channels
            outlay = get_dict(dpath+'outlay3.pkl')
            main = gdf.iloc[:outlay['email']]
            email = gdf.iloc[outlay['email']:outlay['wp']]
            wp = gdf.iloc[outlay['wp']:outlay['seasonal']]
            seasonal = gdf.iloc[outlay['seasonal']:outlay['sms']]
            sms = tab3.filter_na(gdf.iloc[outlay['sms']:])
            del gdf
            self.gdf = tab3.check_new_groups(new_campaigns, main, email, wp,
                                             seasonal, sms, self.t, self.type)
            # tab3.reindex_outlay(self.gdf, self.t)
            del main, email, wp, seasonal, sms

        actual_values = []
        for channel, values in self.t.items():
           for df_trigger in values.keys():
               actual_values.append(df_trigger)
        new_campaigns = list(self.df.name.loc[~self.df.name.isin(actual_values)])
        create_new_campaigns(self.gdf, new_campaigns, self.limit - 1)
        self.gdf = tab3.update_campaigns(self.df, self.gdf, self.limit - 1, self.t, attr)
        self.gdf = tab3.fill_main(self.gdf, self.limit - 1)
        self.format = tab3.build_format_patch(self.gdf, self.limit)
        self.gdf = self.gdf.fillna('')\
            .replace('#DIV/0! (Function DIVIDE parameter 2 cannot be zero.)', '')
        self.gdf.iloc[2:, 4:] = self.gdf.iloc[2:,4:]\
            .astype(str).replace('\.', ',', regex=True)
        return self.gdf

    def get_campaigns_offline(self):
        """Offline campaigns weekly, tab 5 on dashboard"""
        attr = get_dict_value('attribution.pkl', self.type)

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
                val = str(self.df.loc[self.df.name == name, key].values[0])
                pcts = ['click_rate', 'open_rate', 'CTR', 'unfollow_rate',
                        'avg_bill','order_conversion']
                if key in pcts:
                    val = val.replace('.', ',')
                patch.append(gspread.models.Cell(self.limit+ind, column, val))
        return patch


def put_dict(filename, obj):
    with open(filename, 'wb') as f:
        pkl.dump(obj, f, protocol=pkl.HIGHEST_PROTOCOL)


def get_dict(filename):
    with open(filename, 'rb') as f:
        return pkl.load(f)


def update_dictionary(filename, key, value):
    tmp = get_dict(filename)
    tmp[key] = value
    put_dict(filename, tmp)


def get_dict_value(filename, key):
    return get_dict(dpath + filename).get(key)
