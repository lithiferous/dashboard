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

    def __init__(self, df, type, limit):
        """
        Patcher for Google spreadsheets
        -> type: numerical index for spreadsheet
        -> data: dataframe object
        -> limit: max row or column to put new cells
        """
        self.df = df
        self.type = type
        self.limit = limit
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
        self.df = self.df[['name', 'sent', 'delivered', 'opened',
                           'clicked', 'unfollowed', 'revenue', 'orders']]
        pd.reset_option('mode.chained_assignment')
        with pd.option_context('mode.chained_assignment', None):
            self.df['name'] = self.df.name.map(self.t.triggers)
        self.df = self.df[self.df.name.notna()]

        def get_campaign_info(df, name, column):
            """Locates specific value for named campaign"""
            try: res = df.loc[df.name == name, column].values[0]
            except: res = 0
            return res

        patch = []
        keys = self.df.columns[2:]
        for name, rows in self.d[self.type].items():
            for row, key in zip(rows.values(), keys):
                patch.append(gspread.models.Cell(row, self.limit, \
                            str(get_campaign_info(self.df, name, key))))
        return patch

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

    def check_match(self, df, actual_values):
        """Checks whether all known campaigns belong to a dictionary
            -> df: dataframe with first column that needs check (name)
            -> actual_values: values in column (name)
            <- new_values: list of unknown campaigns
        """
        return list(self.df.name.loc[~self.df.name.map(actual_values).notna()])

def check_new_campaigns(campaigns):
    """User interface to update campaign-trigger dictionary
        -> campaigns: list of unknown campaigns
    """
    def get_new_name(name, channel):
        print("""Нажмите только 'Enter', если название компании Вас устраивает:
Введите новое название для отображения в Google Sheets""")
        new_name = input()
        if new_name == '':
            triggers.triggers[channel].update({name:name})
        else:
            triggers.triggers[channel].update({name:new_name})

    n_emails = 0
    n_seasonal = 0
    n_wp = 0

    maxlen = len(campaigns)
    print("""Определите категорию кампании для {n} новых:
Введите 0 для сброса ввода и возврату к первому значению""".format(n=maxlen))

    i = 0
    new_names = []
    while(i != maxlen):
        print("""Нажмите 'Enter' для Email
        '1' для Web-push
        '2' для сезонной
        '3' для SMS
        """)
        v = input('-> ' + campaigns[i] + ': \n')
        if v == '':
            get_new_name(campaigns[i], 'email'); i+=1; n_emails+=1
        elif v == '1':
            get_new_name(campaigns[i], 'wp'); i+=1; n_wp+=1
        elif v == '2':
            get_new_name(campaigns[i], 'seasonal'); i+=1; n_seasonal+=1
        elif v == '3':
            get_new_name(campaigns[i], 'sms'); i+=1;
        elif v == '0':
            return check_new_campaigns(campaigns)
        else:
            print("Повторите ввод в согласии с инструкцией\n")

    return {'email':n_emails,
            'seasonal':n_seasonal,
            'wp':n_wp,}
    #TODO 1) calculate deltas 2) init start dict 3) formatting

def put_dictionary(filename, obj):
    with open(filename, 'wb') as f:
        pkl.dump(obj, f, protocol=pkl.HIGHEST_PROTOCOL)

def get_dictionary(filename):
    with open(filename, 'rb') as f:
        d = pkl.load(f)
    return d
