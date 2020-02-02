from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import re

def build_segmentation(orders):
    """
    Returns strategic segmentation dictionary that counts number of clients on condition
    -> clients_file: mindbox csv with fields: mindbox id, email, external id, registration date
    -> orders: feather dataframe from 'data/cohort/orders.f'
    -> date: datetime
    """
    date = dt.today()
    clients_file = 'data/cohort/clients.csv'
    df = pd.read_csv(clients_file, sep=';', low_memory=False)
    df.columns = ['id_cli', 'email', 'id_cust', 'date_reg']
    df = df.loc[(df.email.notna()) & (df.id_cust.notna())]
    df = df.loc[~df.id_cust.apply(lambda x: True if len(re.findall('^\D', str(x))) != 0 else False)]
    df['date_reg'] = pd.to_datetime(df.date_reg, format="%d.%m.%Y %H:%M:%S")
    df['id_cust'] = df.id_cust.astype('int64')

    res = df.set_index('id_cust').join(orders.set_index('id_cust')).reset_index()
    dusr = {}

    def get_user_count(df, column,
                       dt_min, dt_max,
                       ord_min=None, ord_max=None):
        res = df.loc[(df[column] <= dt_max) & (df[column] >= dt_min)]
        res = res.groupby(['email']).agg({'id_ord':'count'}).reset_index()
        if column == 'date_reg':
            return res.loc[res.id_ord == 0, 'email'].nunique()
        else:
            if ord_min != None:
                res = res.loc[res.id_ord >= ord_min]
            elif ord_max != None:
                res = res.loc[res.id_ord <= ord_max]
            return res.email.nunique()

    # Новые подписчики не покупали и попали в базу <= 14 дней назад
    dt_min = date - timedelta(days=14)
    dt_max = date

    flws_new = get_user_count(res, 'date_reg', dt_min, dt_max)
    dusr['flws_new'] = flws_new
    del flws_new

    # Старые подписчики не покупали и попали в базу > 14 дней назад
    dt_min = res.date_reg.min()
    dt_max = date - timedelta(days=15)

    flws_old = get_user_count(res, 'date_reg', dt_min, dt_max)
    dusr['flws_old'] = flws_old
    del flws_old

    res = res.loc[res.date_ord.notna()]

    # Новые клиенты кол-во покупок от 1 до 3 и последняя <= 7 дней назад
    dt_min = date - timedelta(days=7)
    dt_max = date
    cli_new = get_user_count(res, 'date_ord', dt_min, dt_max, 1, 3)
    dusr['cli_new'] = cli_new
    del cli_new

    # Предотток (новые) кол-во покупок от 1 до 3 и последняя 8 <= x <= 45 дней назад
    dt_min = date - timedelta(days=45)
    dt_max = date - timedelta(days=8)
    prechurn_new = get_user_count(res, 'date_ord', dt_min, dt_max, 1, 3)
    dusr['prechurn_new'] = prechurn_new
    del prechurn_new

    # Предотток (активные) совершили больше 3 покупок и последняя 30 <= x <= 45 дней назад
    dt_min = date - timedelta(days=45)
    dt_max = date - timedelta(days=30)
    prechurn = get_user_count(res, 'date_ord', dt_min, dt_max, 4)
    dusr['prechurn'] = prechurn
    del prechurn

    # Отток (новые) кол-во покупок от 1 до 3 и последняя > 45 дней назад
    dt_min = res.date_reg.min()
    dt_max = date - timedelta(days=46)
    churn_new = get_user_count(res, 'date_ord', dt_min, dt_max, 1, 3)
    dusr['churn_new'] = churn_new
    del churn_new

    # Отток (активные) совершили больше 3 покупок и последняя покупка >45 дней назад
    dt_min = res.date_reg.min()
    dt_max = date - timedelta(days=46)
    churn = get_user_count(res, 'date_ord', dt_min, dt_max, 4)
    dusr['churn'] = churn
    del churn

    # Активные (частые) совершили больше 3 покупок и последняя <= 10 дней назад
    dt_min = date - timedelta(days=10)
    dt_max = date
    active = get_user_count(res, 'date_ord', dt_min, dt_max, 4)
    dusr['active'] = active
    del active

    # Активные (остальные) совершили больше 3 покупок и последняя  10 <= x < 30 дней назад
    dt_min = date - timedelta(days=30)
    dt_max = date - timedelta(days=10)
    active_rest = get_user_count(res, 'date_ord', dt_min, dt_max, 4)
    dusr['active_rest'] = active_rest
    del active_rest

    return dusr
