import numpy as np
import pandas as pd
import re
import zipfile

def get_report(filename, sheet):
    """
    Creates a dataframe from mindbox report
    -> filename: campaign statistics mindbox xlsx
    -> sheet: desired sheet name
    """
    df = pd.read_excel(filename, sheet_name=sheet)
    ind = df.loc[df.iloc[:,0] == 'Бренд'].index[0]
    df = df.iloc[ind+2:, :-1]
    header = ['brand', 'campaign', 'tag', 'date',
          'name', 'channel', 'subject', 'preview_url',
          'sent', 'delivered', 'deliver_rate', 'opened',
          'open_rate', 'clicked', 'click_rate', 'CTR',
          'unfollowed', 'unfollow_rate', 'revenue', 'orders',
          'avg_bill', 'order_conversion']
    df.columns = header
    df[['sent', 'delivered', 'opened', 'clicked',
        'unfollowed', 'revenue', 'orders']] = \
    df[['sent', 'delivered', 'opened', 'clicked',
        'unfollowed', 'revenue', 'orders']].astype('int64')
    df[['deliver_rate', 'open_rate', 'click_rate', 'CTR',
        'unfollow_rate', 'avg_bill', 'order_conversion']] = \
    df[['deliver_rate', 'open_rate', 'click_rate', 'CTR',
        'unfollow_rate', 'avg_bill', 'order_conversion']].astype('float64')
    return df[(df.name != '(Для тестов)') & (df.sent != 0)]

def get_test_control(file_test, file_control):
    """
    Returns a dataframe with columns id_cli, id_cust, group, reg_date
    -> file_test: test client group from MindBox csv
    -> file_control: control client group from MindBox csv
    Both files have fields: MindBox id, External id, Registration date
    """
    def mark_groups(df, group):
        df.columns = ['id_cli', 'id_cust', 'reg_date']
        df['group'] = group
        df = df[df.id_cust.notna()]
        new_series = df.id_cust.apply(lambda x: True if '.' in str(x) else False)
        if sum(new_series) > 0:
            pd.reset_option('mode.chained_assignment')
            with pd.option_context('mode.chained_assignment', None):
                df['id_cust'] = df.id_cust.apply(lambda x: str(x).split('.')[0]).astype('int64')
        else:
            pd.reset_option('mode.chained_assignment')
            with pd.option_context('mode.chained_assignment', None):
                df['id_cust'] = df.id_cust.apply(lambda x: re.sub("[^0-9]", "", str(x)))
                df = df[df.id_cust != '']
                df['id_cust'] = df['id_cust'].astype('int64')
        return df

    test = pd.read_csv(file_test, sep=';', low_memory=False)
    test = mark_groups(test, 'test')
    control = pd.read_csv(file_control, sep=';', low_memory=False)
    control = mark_groups(control, 'control')

    df = test.append(control, ignore_index = True)
    df.to_feather('data/client_groups.f')
    return df

def get_orders(file_orders, file_clients):
    """
    Returns a dataframe of orders for given file_clients ids
    -> file_orders: weekly client orders csv
    -> file_clients: test & control client ids from mindbox csv
    """
    orders = pd.read_csv(file_orders, sep = ',')
    orders.columns = ['id_ord', 'route', 'date', 'date_delivery',
                      'status', 'status_reason', 'delivery',
                      'payment_type','revenue', 'id_cust',
                      'client','phone', 'email']
    orders = orders[['id_ord', 'revenue', 'id_cust']]
    try:
        clients = pd.read_feather(file_clients)
    except:
        clients = get_test_control('data/groups/test.csv', 'data/groups/control.csv')
    clients = clients.join(orders.set_index('id_cust'),on='id_cust')
    clients = clients[clients.revenue.notna()]
    return clients.reset_index(drop=True)

def get_order_info(file_orders, file_clients=None, agg_column=None):
    """
    Creates a dataframe of total orders, revenue, customers for test & control groups.
    """
    if file_clients != None:
        file_clients = file_clients
    else:
        file_clients = 'data/client_groups.f'

    clients = get_orders(file_orders, file_clients)
    if agg_column != None:
        clients = clients.groupby([agg_column])
    clients = clients.agg({'revenue':np.sum,
                           'id_cli':pd.Series.nunique,
                           'id_ord':pd.Series.nunique})
    return clients.astype('int64').reset_index()

def append_new_orders(orders, new_orders):
    """
    Returns a dataframe with all orders from the beginning of client engagement
    -> orders: feather file from 'data/cohort/orders.f'
    -> new_orders: latest client orders.csv file from 'date/orders.csv'
    """
    new_orders = pd.read_csv(new_orders, sep = ';')
    new_orders = new_orders[['ID заказа', 'Дата создания', 'Сумма с учетом доставки, руб.', 'ID Клиента']]
    new_orders.columns = ['id_ord', 'date_ord', 'revenue', 'id_cust']
    pd.reset_option('mode.chained_assignment')
    with pd.option_context('mode.chained_assignment', None):
        new_orders['revenue'] = new_orders.revenue.astype('int64')
        new_orders['date_ord'] = pd.to_datetime(new_orders.date_ord, format=("%d.%m.%y %H:%S"))
        new_orders['date_ord'] = new_orders.date_ord.dt.strftime('%Y-%m-%d')
        new_orders['date_ord'] = pd.to_datetime(new_orders.date_ord, format=("%Y-%m-%d"))
        new_orders = new_orders.sort_values('date_ord').reset_index(drop=True)

    orders = pd.read_feather(orders)
    orders = orders.append(new_orders, ignore_index=True, sort=False)
    orders = orders.loc[~orders.duplicated()].reset_index(drop=True)
    orders = orders.sort_values('date_ord').reset_index(drop=True)
    orders.to_feather('data/cohort/orders.f')
    return orders

def unprotect_excel(excel_file):
    with zipfile.ZipFile(excel_file, 'r') as zip:
        zip.extractall('data/report')
