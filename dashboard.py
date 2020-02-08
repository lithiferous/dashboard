from datetime import datetime, timedelta
import argparse
import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import imp

c = imp.load_source('conf', 'config/conf.py')
g = imp.load_source('gsync', 'modules/gCanvas.py')
s = imp.load_source('patch', 'modules/patcher.py')
r = imp.load_source('report', 'modules/reader.py')


class Dashboard:
    def __init__(self, report_file,
                       orders_file,
                       clients_file,
                       period):
        self.connector = None
        self.report = report_file
        self.orders = None
        self.period = self.get_report_period(period)
        self.orders_file = orders_file

    def get_report_period(self, delta):
        """
        Return period dates in fmt 'DD.MM - DD.MM'
        -> delta: weeks back (4 weeks for month)
        """
        return (datetime.today() - timedelta(weeks=delta)).strftime("%d.%m") \
                +'-'+ datetime.today().strftime("%d.%m")

    def upload_data(self):
        sheets = {'2. Триггеры — инкр. нед.':   1,
                  '3. Триггеры — стат. нед.':   2,
                  '4. Триггеры — инкр. месяц':  3,
                  '5. Регулярка — статистика':  4,
                  '6. Когортный по выручке':    5,
                  '7. Когортный по вовлечению': 6,
                  '8. Страт. сегментация':      7}

        df = r.get_report(self.report, "Свод. данные (online + offline)")
        for sheet in sheets.keys():
            if '2' in sheet:
                sh = g.gCanvas(self.connector.get_sheet_by_name(sheet))
                self.orders = r.get_order_info(self.orders_file, 'group')
                patch = s.Patcher(self.orders, sheets.get(sheet), sh.max_cols)
                sh.update_batch(patch.patch)
            if '3' in sheet:
                data = df[df.channel != 'Ручные рассылки']
                or_sheet = g.gCanvas(self.connector.get_sheet_by_name(sheet))
                sh = g.gCanvas(self.connector.create_sheet(sheet + ' new'))
                gdf = g.gCanvas(self.connector.get_sheet_by_name(sheet)).get_as_df()
                patch = s.Patcher(data, sheets.get(sheet), or_sheet.max_cols, gdf)
                sh.update_with_df(patch.gdf)
                sh.format(patch.format)
            if '4' in sheet:
                p = r.get_order_info(self.orders_file, 'group')
            if '5' in sheet:
                data = df[df.channel == 'Ручные рассылки']
                sh = g.gCanvas(self.connector.get_sheet_by_name(sheet))
                patch = s.Patcher(data, sheets.get(sheet), sh.max_rows)
                sh.update_batch(patch.patch)
            if '7' in sheet:
                sh = g.gCanvas(self.connector.get_sheet_by_name(sheet))
                patch = s.Patcher(self.orders, sheets.get(sheet), sh.max_cols)
                sh.update_batch(patch.patch)

    def run_all(self):
        self.connector = g.Connection(c.name)
        self.upload_data()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('-p', dest='period', required=True, type=int,
                    help="Report period in weeks; Flag 1 - one week")
    p.add_argument('-r', dest="report_file", required=True, type=str,
                    help="MindBox campaigns file xlsx")
    p.add_argument('-o', dest="orders_file", required=True, type=str,
                    help="Orders file csv")
    p.add_argument('-c', dest="clients_file", type=str,
                    help="Clients file feather fmt")
    args = p.parse_args()
    d = Dashboard(args.report_file,
                  args.orders_file,
                  args.clients_file,
                  args.period)
    d.run_all()
